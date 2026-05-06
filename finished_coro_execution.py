import asyncio


async def victim():
    x = "alive"
    await asyncio.sleep(0)
    print("resumed, x =", x)


async def attacker(t):
    coro = t.get_coro()
    coro.send(None)


async def main():
    t = asyncio.create_task(victim())
    await asyncio.sleep(0)
    asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(attacker(t)))
    await t


asyncio.run(main())
