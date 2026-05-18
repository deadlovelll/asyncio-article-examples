import asyncio
import hashlib
from dataclasses import dataclass
import json
from block_detector_step4 import detect_blocking


@dataclass
class Order:
    order_id: int
    items: list[str]
    user_id: int


async def fetch_orders(user_id: int) -> list[Order]:
    await asyncio.sleep(0.05)
    return [
        Order(order_id=i, items=[f"item_{j}" for j in range(200)], user_id=user_id)
        for i in range(200)
    ]


def compute_order_fingerprints(orders: list[Order]) -> dict[int, str]:
    result = {}
    for order in orders:
        raw = json.dumps({
            "order_id": order.order_id,
            "items": order.items * 1000,
            "user_id": order.user_id,
        }, sort_keys=True).encode()
        result[order.order_id] = hashlib.sha256(raw).hexdigest()
    return result


@detect_blocking(threshold=0.1, action="raise")
async def process_user_orders(user_id: int) -> dict[int, str]:
    orders = await fetch_orders(user_id)
    fingerprints = compute_order_fingerprints(orders)
    return fingerprints

async def process_next():
    await asyncio.sleep(1)

async def main() -> None:
    result = await process_user_orders(user_id=1)
    print(f"Обработано заказов: {len(result)}")
    await process_next()


asyncio.run(main())
