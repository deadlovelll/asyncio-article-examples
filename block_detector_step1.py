import asyncio
import time
from asyncio import AbstractEventLoop
from threading import Thread, Event


class LoopBlockDetector:
    __slots__ = (
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

    def start(self):
        self._running = True
        self._last_ping = time.monotonic()
        self._stop_event.clear()
        self._schedule_ping()

    def stop(self):
        self._running = False
        self._stop_event.set()


async def main():
    loop: AbstractEventLoop = asyncio.get_running_loop()
    detector: LoopBlockDetector = LoopBlockDetector(loop, threshold=0.1)
    detector.start()
    print("[detector] запущен")

    print("\n--- нормальная работа ---")
    await asyncio.sleep(0.3)

    print("\n--- блокируем loop на 500 мс ---")
    time.sleep(0.5)

    await asyncio.sleep(0.3)

    detector.stop()
    print("[detector] остановлен")


asyncio.run(main())
