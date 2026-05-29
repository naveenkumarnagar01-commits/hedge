import urllib.request, json

d = json.loads(urllib.request.urlopen('http://localhost:8000/api/options-chain').read())
print('Chain count:', len(d.get('chain', [])))
print('Hours left:', d.get('hours_left'))
print('Source:', d.get('source'))
print()
for o in d.get('chain', []):
    print(f"  {o['side']} Strike={o['strike']} Bid={o['bid']} Ask={o['ask']} BidQty={o['bid_qty']} AskQty={o['ask_qty']} Mark={o['mark']} Intr={o['intrinsic']} Source={o['source']}")
