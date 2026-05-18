from typing import Any, Coroutine, Generator


class Reenter:
    def __init__(self, target: Coroutine[Any, Any, None]) -> None:
        self.target: Coroutine[Any, Any, None] = target

    def __await__(self) -> Generator[None, None, None]:
        self.target.send(None)
        yield


async def victim() -> None:
    x: int = 123
    await Reenter(c)
    print("after, x =", x)


c: Coroutine[Any, Any, None] = victim()
c.send(None)
