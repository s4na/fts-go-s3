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
for key in ('gets_per_query', 'bytes_per_query', 'single_update_puts', 'manifest_bytes'):
    before, after = baseline[key], current[key]
    print(f'| {key} | {before:.2f} | {after:.2f} | {after-before:+.2f} |')
