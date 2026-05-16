import asyncio
import linecache
import logging
import sys
import time
import traceback
import warnings
from asyncio import AbstractEventLoop
from threading import Event, Thread, current_thread


class LoopBlockDetector:
    __slots__ = (
        "_action",
        "_fired",
        "_loop",
        "_threshold",
        "_last_ping",
        "_running",
        "_stop_event",
        "_thread",
        "_task",
        "_loop_thread_id",
    )

    def __init__(
        self,
        loop: AbstractEventLoop,
        threshold: float,
        loop_thread_id: int,
        action: str,
        task: asyncio.Task[None],
    ) -> None:

        self._loop: AbstractEventLoop = loop
        self._threshold: float = threshold
        self._last_ping: float = time.monotonic()
        self._running: bool = False
        self._stop_event: Event = Event()
        self._thread: Thread | None = None
        self._fired: bool = False
        self._loop_thread_id: int = loop_thread_id
        self._action: str = action
        self._task: asyncio.Task[None] = task

    def _schedule_ping(self):
        if self._running:
            now: float = time.monotonic()
            lag: float = now - self._last_ping
            print(f"[ping] такт, отставание: {lag * 1000:.1f} мс")
            self._last_ping = now
            self._loop.call_later(
                self._threshold / 2,
                self._schedule_ping,
            )

    def _capture_stack(self) -> str:
        frames = sys._current_frames()
        frame = frames.get(self._loop_thread_id)
        if frame is None:
            return "<стектрейс недоступен>"

        stack = traceback.extract_stack(frame)

        SKIP_PREFIXES = (
            "asyncio/",
            "asyncio\\",
            "_detect_blocking",
            "detect_blocking",
            "threading",
        )
        filtered = [
            f for f in stack if not any(skip in f.filename for skip in SKIP_PREFIXES)
        ]

        if not filtered:
            filtered = stack

        lines: list[str] = ["Стек в момент блокировки:\n"]
        for frame_info in filtered:
            line_src: str = linecache.getline(
                frame_info.filename, frame_info.lineno
            ).strip()
            lines.append(
                f'  File "{frame_info.filename}", line {frame_info.lineno}, in {frame_info.name}\n    {line_src}\n'
            )

        if filtered:
            culprit = filtered[-1]
            lines.append(
                f"\n→ Виновник: {culprit.name}() в {culprit.filename}:{culprit.lineno}"
            )

        return "".join(lines)

    def _watchdog(self):
        while not self._stop_event.wait(timeout=self._threshold):
            if self._fired:
                break
            lag = time.monotonic() - self._last_ping
            if lag > self._threshold:
                stack_trace = self._capture_stack()
                msg = (
                    f"\n{'=' * 60}\n"
                    f"[EventLoopBlock] loop заблокирован "
                    f"на {lag:.3f}s (порог: {self._threshold}s)\n"
                    f"{stack_trace}\n"
                    f"{'=' * 60}"
                )
                if self._action == "raise" and not self._fired:
                    self._fired = True
                    self._loop.call_soon_threadsafe(self._cancel_task, msg)
                elif self._action == "warn":
                    warnings.warn(msg, RuntimeWarning, stacklevel=2)
                elif self._action == "log":
                    logging.getLogger("event_loop_block").warning(msg)

    def _cancel_task(self, msg: str):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel(msg)

    def start(self):
        self._running = True
        self._fired = False
        self._last_ping = time.monotonic()
        self._stop_event.clear()
        self._schedule_ping()
        self._thread = Thread(target=self._watchdog, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._threshold * 2)


def blocking_op() -> None:
    time.sleep(0.5)


async def main():
    loop = asyncio.get_running_loop()
    loop_thread_id = current_thread().ident
    current_task = asyncio.current_task()
    detector = LoopBlockDetector(
        loop,
        threshold=0.1,
        loop_thread_id=loop_thread_id,  # pyright: ignore[reportArgumentType]
        action="raise",
        task=current_task,  # pyright: ignore[reportArgumentType]
    )
    detector.start()
    print("[detector] запущен")
    print("[watchdog] сторожевой поток запущен")

    print("\n--- нормальная работа ---")
    await asyncio.sleep(0.3)

    print("\n--- блокируем цикл событий на 500 мс ---")
    blocking_op()

    # даём циклу событий восстановиться
    await asyncio.sleep(0.3)

    detector.stop()
    print("[detector] остановлен")


asyncio.run(main())
