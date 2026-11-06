"""Render the measured payload comparison for the Actions job summary."""
import json
import sys

with open(sys.argv[1], encoding='utf-8') as f:
    metrics = json.load(f)

full = metrics['source_document_bytes']
index = metrics['bytes_per_query']
fraction = metrics['index_to_full_scan_fraction']
inverse = metrics['full_scan_to_index_ratio']
print('## 読み込み量は全文走査の何分の 1？\n')
print(f'- 全文書の本文を 1 回ずつ読む場合: {full:,} バイト／クエリ')
print(f'- 索引検索で読んだデータ: 平均 {index:,.0f} バイト／クエリ')
if fraction is None:
    print('- 割合: 全文書の本文が 0 バイトのため算出できません')
elif inverse is None:
    print('- 割合: 0%（索引の読み込みが 0 バイトのため逆数は算出できません）')
elif inverse >= 1:
    print(f'- 割合: 約 1 / {inverse:,.2f}（全文走査の {fraction * 100:.2f}%）')
else:
    print(f'- 割合: 全文走査の {fraction * 100:.2f}%（全文走査より多い）')
print('\n文書のサイズは、索引に渡した UTF-8 テキストのバイト数の合計です。'
      '検索の読み込み量はストアの GET 応答データで、索引のマニフェスト等を含みます。'
      'HTTP ヘッダーや、候補を確定するための元文書の読み取りは含みません。'
      '同じ Index を全クエリで使うため、最初のクエリ以降は索引の目次キャッシュを利用します。')

operations = metrics.get('operations') or {}
if operations:
    print('\n## 操作ごとのピークヒープ\n')
    print('| 操作 | 秒 | ピーク増分 (MiB) | 割り当て累計 (MiB) | GET | 受信 (MiB) | PUT |')
    print('|---|---:|---:|---:|---:|---:|---:|')
    for name, op in sorted(operations.items()):
        print(f"| {name} | {op['seconds']:.2f} | {op['peak_heap_delta_bytes'] / 2**20:,.1f} | "
              f"{op['alloc_bytes'] / 2**20:,.1f} | {op['gets']:,} | {op['received_bytes'] / 2**20:,.1f} | {op['puts']:,} |")
    print('\nピーク増分は、操作開始時に GC した後のヒープ使用量からの増加分の最大値です（1 ms 間隔の標本、未回収のゴミを含む）。'
          '各操作は新しい Index を開くため、前の操作のキャッシュは含みません。'
          'build は Apply に渡すバッチ分の本文を含みます。')
