async def victim():
    x = "alive"
    return x


c = victim()

try:
    c.send(42) 
except StopIteration as e:
    print("stopped, value =", e.value)