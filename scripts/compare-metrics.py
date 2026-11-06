import json
import sys

with open(sys.argv[1], encoding='utf-8') as f:
    baseline = json.load(f)
with open(sys.argv[2], encoding='utf-8') as f:
    current = json.load(f)
print('\n### Comparison with main\n')
if any(baseline.get(key) != current.get(key) for key in ('documents', 'queries', 'batch_size')):
    print('The main measurement used a different corpus or segment batch size; no direct comparison is available.')
    sys.exit(0)
print('| Metric | Main | PR | Difference |')
print('|---|---:|---:|---:|')
for key in ('gets_per_query', 'bytes_per_query', 'single_update_puts', 'manifest_bytes', 'index_to_full_scan_fraction'):
    before, after = baseline.get(key), current.get(key)
    if before is None or after is None:
        before_text = '未計測' if before is None else f'{before:.4f}'
        after_text = '未計測' if after is None else f'{after:.4f}'
        print(f'| {key} | {before_text} | {after_text} | — |')
        continue
    print(f'| {key} | {before:.4f} | {after:.4f} | {after-before:+.4f} |')

before_ops, after_ops = baseline.get('operations') or {}, current.get('operations') or {}
if after_ops:
    print('\n#### Peak heap delta (MiB)\n')
    print('| Operation | Main | PR | Difference |')
    print('|---|---:|---:|---:|')
    for name in sorted(after_ops):
        after = after_ops[name]['peak_heap_delta_bytes'] / 2**20
        if name not in before_ops:
            print(f'| {name} | 未計測 | {after:.1f} | — |')
            continue
        before = before_ops[name]['peak_heap_delta_bytes'] / 2**20
        print(f'| {name} | {before:.1f} | {after:.1f} | {after-before:+.1f} |')
