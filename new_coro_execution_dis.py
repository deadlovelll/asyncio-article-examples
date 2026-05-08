import dis


async def victim():
    x = "alive"
    return x


print("=== bytecode of victim ===")
dis.dis(victim)
print()

c = victim()

try:
    c.send(42)
except StopIteration as e:
    print("stopped, value =", e.value)
