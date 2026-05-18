from typing import Any, Coroutine


async def victim() -> str:
    x: str = "alive"
    return x


c: Coroutine[Any, Any, str] = victim()

try:
    c.send(42)
except StopIteration as e:
    print("stopped, value =", e.value)
