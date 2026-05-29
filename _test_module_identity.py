"""Test: does python -m set sys.modules["backend.main"]?"""
import sys

print(f"__name__: {__name__}")
print(f"backend.main in sys.modules at startup: {'backend.main' in sys.modules}")

# Now simulate what gateway.py does
import backend.main as m

print(f"After import backend.main:")
print(f"  m is sys.modules['__main__']: {m is sys.modules.get('__main__')}")
print(f"  id(__main__): {id(sys.modules.get('__main__'))}")
print(f"  id(m): {id(m)}")
print(f"  m.bullish.current_price: {m.bullish.current_price}")
print(f"  id(m.bullish): {id(m.bullish)}")
