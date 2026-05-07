class Reenter:
    def __init__(self, target):
        self.target = target

    def __await__(self):
        self.target.send(None)
        yield


async def victim():
    x = 123
    await Reenter(c)
    print("after, x =", x)


c = victim()
c.send(None)
