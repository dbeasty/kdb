#!/usr/bin/env python3
"""report.py <results.jsonl> <labelA,labelB,...>: mean ops/s per cell and label, every pass in
brackets, the last label against the first, and write p99 / p99.9 / max per pass (ms)."""
import collections, json, sys

rows = [json.loads(l) for l in open(sys.argv[1])]
labels = sys.argv[2].split(',')
by = collections.defaultdict(list)
order = []
for r in rows:
    label = r['label'].rsplit('-', 1)[0]  # run.sh appends -fast / -durable
    key = (r['cell'], r['concurrency'])
    if key not in order:
        order.append(key)
    by[key + (label,)].append(r)

def mean(rs):
    return sum(x['opsPerSec'] for x in rs) / len(rs) if rs else 0

print('| cell | c | ' + ' | '.join(labels) + f' | {labels[-1]}/{labels[0]} | write p99 / p99.9 / max ms |')
print('|---|--:|' + '--:|' * len(labels) + '--:|---|')
for cell, c in order:
    cols, tails = [], []
    for l in labels:
        rs = by.get((cell, c, l), [])
        cols.append(f"{mean(rs):,.0f} ({' / '.join(f'{x['opsPerSec']:,.0f}' for x in rs)})")
        tails.append(l + ': ' + ', '.join(
            f"{x['writeP99us']/1000:.1f}/{x.get('writeP999us', 0)/1000:.1f}/{x.get('writeMaxUs', 0)/1000:.0f}" for x in rs))
    first, last = mean(by.get((cell, c, labels[0]), [])), mean(by.get((cell, c, labels[-1]), []))
    ratio = f'{last/first:.2f}x' if first else '-'
    tail = '; '.join(tails) if cell != 'read' else ''
    print(f'| {cell} | {c} | ' + ' | '.join(cols) + f' | {ratio} | {tail} |')
