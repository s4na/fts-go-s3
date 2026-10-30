#!/usr/bin/env python3
"""Create the pinned Wikipedia/Aozora oracle corpus for CI."""
import argparse
import json
from pathlib import Path

import pyarrow.parquet as parquet

parser = argparse.ArgumentParser()
parser.add_argument('--wiki', required=True)
parser.add_argument('--aozora', action='append', default=[])
parser.add_argument('--count', type=int, default=1000)
parser.add_argument('--output', required=True)
args = parser.parse_args()

written = 0
with open(args.output, 'w', encoding='utf-8') as output:
    for batch in parquet.ParquetFile(args.wiki).iter_batches(batch_size=256, columns=['id', 'title', 'text']):
        for row in batch.to_pylist():
            if written == args.count:
                break
            doc = {'id': 'wiki:' + str(row['id']), 'text': row['title'] + '\n' + row['text']}
            output.write(json.dumps(doc, ensure_ascii=False) + '\n')
            written += 1
        if written == args.count:
            break
    if written != args.count:
        raise RuntimeError(f'expected {args.count} Wikipedia documents, got {written}')
    for name in args.aozora:
        path = Path(name)
        text = path.read_bytes().decode('shift_jis')
        output.write(json.dumps({'id': 'aozora:' + path.stem, 'text': path.stem + '\n' + text}, ensure_ascii=False) + '\n')
