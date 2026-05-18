import asyncio
import time

from pydantic import BaseModel


class Item(BaseModel):
    id: int
    name: str
    description: str


async def heavy_serialization() -> None:
    items: list[Item] = [
        Item(id=i, name=f"Item {i}", description="x" * 10_000)
        for i in range(2_000_000)
    ]
    start: float = time.perf_counter()
    dumped: list[dict[str, object]] = [item.model_dump(by_alias=True) for item in items]

    elapsed: float = time.perf_counter() - start
    print(f"Serialization done in {elapsed:.2f}s, items: {len(dumped)}")


async def heartbeat() -> None:
    while True:
        print(f"[{time.strftime('%X')}] Heartbeat")
        await asyncio.sleep(0.5)


async def main() -> None:
    async with asyncio.TaskGroup() as tg:
        tg.create_task(heartbeat())
        tg.create_task(heavy_serialization())


asyncio.run(main())
