#!/usr/bin/env python3
"""Convert a pinned Japanese Wikipedia parquet shard and Aozora texts to JSONL."""
import argparse
import collections
import json
import pathlib
import unicodedata

import pyarrow.parquet as pq


def normalize(text):
    return unicodedata.normalize("NFKC", text).lower()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--aozora", action="append", default=[])
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    samples = []
    count = 0
    source = pq.ParquetFile(args.parquet)
    with (out / "documents.jsonl").open("w", encoding="utf-8") as f:
        for batch in source.iter_batches(batch_size=1024, columns=["id", "title", "text"]):
            for row in batch.to_pylist():
                if count == args.count:
                    break
                doc = {"id": "wiki:" + str(row["id"]), "title": row["title"], "text": row["text"]}
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                if count < 200:
                    samples.append(doc)
                count += 1
            if count == args.count:
                break
        if count != args.count:
            raise RuntimeError(f"requested {args.count}, found only {count} Wikipedia documents")
        for path in args.aozora:
            p = pathlib.Path(path)
            doc = {"id": "aozora:" + p.stem, "title": p.stem, "text": p.read_bytes().decode("shift_jis")}
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            samples.append(doc)
    queries = []
    def add(kind, value):
        if value:
            queries.append({"kind": kind, "query": value})
    for doc in samples[:20]:
        add("title", doc["title"])
    for doc in samples[:50]:
        text = normalize(doc["text"])
        if len(text) >= 10:
            for length in (2, 5, 10):
                start = len(text) // 2
                add("substring", text[start:start + length])
    terms = collections.Counter()
    for doc in samples[:200]:
        text = normalize(doc["title"] + "\n" + doc["text"])
        terms.update(set(text[i:i + 2] for i in range(len(text) - 1)))
    if terms:
        ranked = sorted(terms.items(), key=lambda item: (-item[1], item[0]))
        add("frequent", ranked[0][0])
        add("rare", ranked[-1][0])
    for kind, query in [("single", "猫"), ("mixed", "ABC-123"), ("width", "ＡＢＣ１２３"), ("no_hit", "𪛖𪛖𪛖𪛖𪛖𪛖")]:
        add(kind, query)
    (out / "queries.json").write_text(json.dumps(queries, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {count} Wikipedia documents and {len(args.aozora)} Aozora works; {len(queries)} queries")


if __name__ == "__main__":
    main()
