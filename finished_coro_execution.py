import asyncio
from typing import Any, Coroutine


async def victim() -> None:
    x: str = "alive"
    await asyncio.sleep(0)
    print("resumed, x =", x)


async def attacker(t: asyncio.Task[None]) -> None:
    coro: Coroutine[Any, Any, None] = t.get_coro()
    coro.send(None)


async def main() -> None:
    t: asyncio.Task[None] = asyncio.create_task(victim())
    await asyncio.sleep(0)
    asyncio.get_running_loop().call_soon(
        lambda: asyncio.ensure_future(attacker(t))
    )
    await t


asyncio.run(main())
