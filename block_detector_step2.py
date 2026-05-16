import asyncio
import time
from asyncio import AbstractEventLoop
from threading import Event, Thread


class LoopBlockDetector:
    __slots__ = (
        "_fired",
        "_loop",
        "_threshold",
        "_last_ping",
        "_running",
        "_stop_event",
        "_thread",
    )

    def __init__(self, loop: AbstractEventLoop, threshold: float) -> None:
        self._loop: AbstractEventLoop = loop
        self._threshold: float = threshold
        self._last_ping: float = time.monotonic()
        self._running: bool = False
        self._stop_event: Event = Event()
        self._thread: Thread | None = None
        self._fired: bool = False

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

    def _watchdog(self):
        while not self._stop_event.wait(timeout=self._threshold):
            if self._fired:
                break
            lag = time.monotonic() - self._last_ping
            if lag > self._threshold:
                self._fired = True
                print(
                    f"[watchdog] блокировка обнаружена: "
                    f"отставание {lag*1000:.1f} мс"
                )

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


async def main():
    loop = asyncio.get_running_loop()
    detector = LoopBlockDetector(loop, threshold=0.1)
    detector.start()
    print("[detector] запущен")
    print("[watchdog] сторожевой поток запущен")

    print("\n--- нормальная работа ---")
    await asyncio.sleep(0.3)

    print("\n--- блокируем цикл событий на 500 мс ---")
    time.sleep(0.5)

    # даём циклу событий восстановиться
    await asyncio.sleep(0.3)

    detector.stop()
    print("[detector] остановлен")

asyncio.run(main())