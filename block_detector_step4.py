import asyncio
import functools
import linecache
import logging
import sys
import threading
import time
import traceback
import warnings
from asyncio import AbstractEventLoop, Task
from traceback import FrameSummary, StackSummary
from types import FrameType
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar

BLOCKING_THRESHOLD: float = 0.1

P = ParamSpec("P")
T = TypeVar("T")

AsyncFunc = Callable[P, Coroutine[Any, Any, T]]


def detect_blocking(
    threshold: float = 0.1, action: str = "raise"
) -> Callable[[AsyncFunc[P, T]], AsyncFunc[P, T]]:
    def decorator(func: AsyncFunc[P, T]) -> AsyncFunc[P, T]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            loop: AbstractEventLoop = asyncio.get_running_loop()
            current_task: Task[T] | None = asyncio.current_task()
            loop_thread_id: int | None = threading.current_thread().ident
            if current_task is None or loop_thread_id is None:
                raise RuntimeError(
                    "detect_blocking должен использоваться внутри event loop"
                )
            blocker: LoopBlockDetector = LoopBlockDetector(
                loop,
                threshold,
                action,
                func.__name__,
                current_task,
                loop_thread_id,
            )
            blocker.start()
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError as e:
                if action == "raise":
                    raise RuntimeError(str(e)) from None
                raise
            finally:
                blocker.stop()

        return wrapper

    return decorator


class LoopBlockDetector:
    __slots__ = (
        "_loop",
        "_threshold",
        "_action",
        "_func_name",
        "_task",
        "_loop_thread_id",
        "_last_ping",
        "_running",
        "_fired",
        "_stop_event",
        "_thread",
    )

    def __init__(
        self,
        loop: AbstractEventLoop,
        threshold: float,
        action: str,
        func_name: str,
        task: Task[Any],
        loop_thread_id: int,
    ) -> None:

        self._loop: AbstractEventLoop = loop
        self._threshold: float = threshold
        self._action: str = action
        self._func_name: str = func_name
        self._task: Task[Any] = task
        self._loop_thread_id: int = loop_thread_id
        self._last_ping: float = time.monotonic()
        self._running: bool = False
        self._fired: bool = False
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None

    def _capture_stack(self) -> str:
        frames: dict[int, FrameType] = sys._current_frames()
        frame: FrameType | None = frames.get(self._loop_thread_id)
        if frame is None:
            return "<стектрейс недоступен>"

        stack: StackSummary = traceback.extract_stack(frame)

        SKIP_PREFIXES: tuple[str, ...] = (
            "asyncio/",
            "asyncio\\",
            "_detect_blocking",
            "detect_blocking",
            "threading",
        )
        filtered: list[FrameSummary] = [
            f for f in stack if not any(skip in f.filename for skip in SKIP_PREFIXES)
        ]

        if not filtered:
            filtered = list(stack)

        lines: list[str] = ["Стек в момент блокировки:\n"]
        for frame_info in filtered:
            line_src: str = linecache.getline(
                frame_info.filename, frame_info.lineno or 0
            ).strip()
            lines.append(
                f'  File "{frame_info.filename}", line {frame_info.lineno}, '
                f"in {frame_info.name}\n    {line_src}\n"
            )

        if filtered:
            culprit: FrameSummary = filtered[-1]
            lines.append(
                f"\n→ Виновник: {culprit.name}() в {culprit.filename}:{culprit.lineno}"
            )

        return "".join(lines)

    def _schedule_ping(self) -> None:
        if self._running:
            self._last_ping = time.monotonic()
            self._loop.call_later(self._threshold / 2, self._schedule_ping)

    def _watchdog(self) -> None:
        while not self._stop_event.wait(timeout=self._threshold):
            if self._fired:
                break
            lag: float = time.monotonic() - self._last_ping
            if lag > self._threshold:
                stack_trace: str = self._capture_stack()
                msg: str = (
                    f"\n{'=' * 60}\n"
                    f"[EventLoopBlock] '{self._func_name}' заблокировал loop "
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

    def _cancel_task(self, msg: str) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel(msg)

    def start(self) -> None:
        self._running = True
        self._fired = False
        self._last_ping = time.monotonic()
        self._stop_event.clear()
        self._schedule_ping()
        self._thread = threading.Thread(target=self._watchdog, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._threshold * 2)
