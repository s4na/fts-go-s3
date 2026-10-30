# zenbun

zenbun は S3 互換ストレージ上の UTF-8 文書に対する部分文字列検索の候補キーを返す Go ライブラリです。ドキュメントとは別のプレフィックスにインデックスを置きます。NFKC 正規化・小文字化した文字列を uni-gram / bi-gram で検索します。候補には誤候補が含まれるため、厳密な一致が必要な場合は返されたキーの本文を読み、同じ正規化を施して `strings.Contains` で確認してください。順位付けは行いません。

## 利用例

```go
store, err := zenbun.NewS3Store(zenbun.S3Config{
    Endpoint: "https://s3.example.com", Bucket: "example", Region: "us-east-1",
})
if err != nil { return err }
index, err := zenbun.Open(store, "indexes/articles/")
if err != nil { return err }
if err := zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
    return index.Apply(ctx, []zenbun.Change{{Key: "articles/1.txt", Data: []byte("日本語の本文")}})
}); err != nil { return err }
keys, err := index.Search(ctx, "日本語")
```

認証情報は `S3Config` または `AWS_ACCESS_KEY_ID`・`AWS_SECRET_ACCESS_KEY`・`AWS_SESSION_TOKEN` から渡します。IAM ロールや IMDS は対応していません。`VirtualHost` でバーチャルホスト形式に切り替えられます。デフォルトの HTTP タイムアウトは 30 秒です。カスタムクライアントを渡す場合は呼び出し側でタイムアウトを設定してください。読み取りは一時的なエラーに対し指数バックオフで再試行します。書き込み・削除は結果不明時の重複や競合を避けるため自動再試行しません。

`Apply` はバッチの追加・更新・削除を公開し、同じマニフェストを基にした競合は `ErrConflict` を返します。`RetryConflict` のコールバックでは毎回必要な状態を読み直してください。`Sync` は指定プレフィックスを ETag で比較し、追加・更新・削除を一括反映します。インデックスは同期元プレフィックス専用として使い、同期中の元文書の変更は呼び出し側で調停してください。一覧と取得の間に ETag が変われば `ErrConflict` を返します。

## CLI

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
export S3_ENDPOINT=https://s3.example.com S3_BUCKET=example AWS_REGION=us-east-1
zenbun sync -source articles/ -index indexes/articles/
zenbun search -index indexes/articles/ '日本語'
zenbun compact -index indexes/articles/
zenbun reclaim -index indexes/articles/ -grace 24h
```

サブコマンドごとに `-endpoint`、`-bucket`、`-region`、`-virtual-host` を指定できます。検索結果は一行一キーです。キーに改行を含める場合は CLI の行形式では曖昧になるため Go API を使ってください。

## 運用と制限

- ストアは強い整合性のある Get・List と、原子的な `If-None-Match` / `If-Match` を備えている必要があります。[互換性一覧](docs/compatibility.md)を参照してください。
- `Compact` はサイズの近い隣接セグメントを 1 組ずつ統合します。CLI は統合できなくなるまで繰り返します。セグメントが増えるとマニフェストと検索コストも増えるため、定期的な実行が必要です。
- `Reclaim` の最初の実行は未参照オブジェクトに印を付け、猶予時間を過ぎた後の実行で削除します。猶予時間は **検索・コンパクションなど読み取りの最大時間、時計差、ストレージ一覧の遅延の合計より長く**してください。上限を保証できない場合は安全な回収はできません。異なる回収実行者でも同じ前提を守ってください。
- 文書とキーは UTF-8、クエリは空でない UTF-8 とします。マニフェスト、辞書ブロック、辞書目次および各エントリーの上限はそれぞれ 64 MiB です。上限を超える書き込みは公開前にエラーになります。
- インデックスのマニフェストは文書一覧ではなくセグメント一覧を持ちます。更新ごとにセグメントが増えるため、サイズはセグメント数に比例します。1 件更新の通常の PUT はセグメントとマニフェストの 2 回です。
- 保存形式の詳細と互換性は[仕様書](docs/format.md)を参照してください。別バージョンの形式は拒否します。
- 暫定性能目標の 100,002 文書での転送量は未測定です。CI は 1,002 文書・178 クエリの GET 数、受信量、更新 PUT 数、マニフェストサイズを記録します。main に同じ計測結果があれば PR と比較します。

## ライセンス

[MIT License](LICENSE)（Copyright (c) 2026 s4na）
