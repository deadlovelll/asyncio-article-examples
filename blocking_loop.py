import asyncio
from pydantic import BaseModel
import time

class Item(BaseModel):
    id: int
    name: str
    description: str

async def heavy_serialization():
    items = [
        Item(id=i, name=f"Item {i}", description="x" * 10_000)
        for i in range(2_000_000)
    ]
    start = time.perf_counter()
    dumped = [item.model_dump(by_alias=True) for item in items]

    elapsed = time.perf_counter() - start
    print(f"Serialization done in {elapsed:.2f}s")

async def heartbeat():
    while True:
        print(f"[{time.strftime('%X')}] Heartbeat")
        await asyncio.sleep(0.5)

async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(heartbeat())
        tg.create_task(heavy_serialization())

asyncio.run(main())