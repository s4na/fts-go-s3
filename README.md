# zenbun

zenbun は、S3 互換ストレージに置いたテキスト文書から、指定した文字列を含む**可能性がある文書のキー**を探す Go ライブラリです。コマンドラインからも使えます。

たとえば `articles/a.txt` に「日本語の文書」という本文があるとき、「日本語」を検索すると、その文書のキーを候補として受け取れます。文書を更新・削除した後も、インデックス全体を作り直さずに変更を反映できます。

> **検索結果は、本文そのものでも、完全一致を保証する結果でもありません。** zenbun は取りこぼしを避けるために候補を広めに返します。最終的に一致するかどうかは、呼び出し側が元の文書を読んで確認してください。

## 何をするもの？

S3 のバケットを大きな本棚、オブジェクトのキーを本の置き場所だと考えてください。文書が増えるたびに本を全部開いて調べるのは大変です。zenbun は、本の中にどんな文字列があるかを記録した**索引**を別の場所に作ります。

```text
バケット
├── articles/                   元の文書
│   ├── a.txt                   「日本語の文書」
│   └── b.txt                   「英語の文書」
└── indexes/articles/           zenbun が管理する索引
```

検索時には索引の必要な部分を読み、候補のキーを返します。元の本文は検索のたびに読みません。候補の中に紛れ込む「文字は含むが、並び方が違う」文書は、最後に本文を確認することで取り除けます。

文字の比べ方は、幅などの違いを整える **NFKC 正規化**を行い、その後に小文字化します。たとえば `ＡＢＣ` と `abc` は同じ形になります。位置情報やランキングは持たないため、検索結果の順番は関連度を表しません。

## はじめに確認すること

- **文書は UTF-8 のテキスト**にしてください。画像や PDF のテキスト抽出は行いません。
- **文書と索引には、重ならないプレフィックスを使ってください。** プレフィックスは `articles/` のようなキーの共通の先頭部分です。元の文書と zenbun の管理データを分けます。
- ストレージには、部分読み取り（Range GET）、一覧、削除、原子的な条件付き PUT、強い読み取り・一覧整合性が必要です。CI では VersityGW と SeaweedFS を検証しています。Amazon S3 自体はまだ検証していません。詳しくは[ストレージ互換性](docs/compatibility.md)を参照してください。
- Go のバージョンは **1.25 以上**が必要です。

## まずはメモリ上で試す

S3 を用意しなくても、メモリストアを使って検索の流れを試せます。次の例のデータはプロセス終了時に消えます。

新しいディレクトリで試すなら、先に Go モジュールを作り、ライブラリを追加します。

```sh
mkdir zenbun-demo
cd zenbun-demo
go mod init example.com/zenbun-demo
go get github.com/s4na/fts-go-s3
```

以下を `main.go` として保存し、`go run .` で実行します。

```go
package main

import (
    "context"
    "fmt"
    "log"

    zenbun "github.com/s4na/fts-go-s3"
)

func main() {
    ctx := context.Background()
    store := &zenbun.MemoryStore{}

    index, err := zenbun.Open(store, "indexes/articles/")
    if err != nil {
        log.Fatal(err)
    }

    // 本文と、その置き場所を索引に登録する。
    err = index.Apply(ctx, []zenbun.Change{
        {Key: "articles/a.txt", Data: []byte("日本語の文書")},
        {Key: "articles/b.txt", Data: []byte("英語の文書")},
    })
    if err != nil {
        log.Fatal(err)
    }

    keys, err := index.Search(ctx, "日本語")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(keys) // [articles/a.txt]
}
```

`Apply` は索引を更新します。**指定した `Data` を元のストレージに保存する操作ではありません。** 実際の文書の保存・更新・削除は呼び出し側で行ってください。

## S3 互換ストレージで使う

既存の文書群を対象にする場合、`Sync` は文書一覧と ETag（オブジェクトの変更を見分ける値）を使い、追加・更新・削除を索引へ反映します。

**1 つの索引を 1 つの同期元プレフィックス専用にしてください。** `Sync` は、同期元の一覧に存在しない索引内のキーを削除します。別の同期元のキーや `Apply` で手動登録したキーを同じ索引に混ぜると、それらも削除されることがあります。

まず、利用する S3 互換サービスのエンドポイント・バケット・認証情報を用意します。バケットと `articles/` 以下の UTF-8 文書は、あらかじめ用意してください。

```sh
export S3_ENDPOINT='https://s3.example.com'
export S3_BUCKET='example-bucket'
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY_ID='自分のアクセスキー'
export AWS_SECRET_ACCESS_KEY='自分のシークレットキー'
# 一時的な認証情報を使うときだけ設定します。
# export AWS_SESSION_TOKEN='自分のセッショントークン'
```

値は自分の環境に合わせて置き換えてください。認証情報はリポジトリに書き込まないでください。

次の例は、文書を同期してから「日本語」の候補を表示します。エンドポイントなどは環境変数から読みます。

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    zenbun "github.com/s4na/fts-go-s3"
)

func main() {
    ctx := context.Background()
    store, err := zenbun.NewS3Store(zenbun.S3Config{
        Endpoint: os.Getenv("S3_ENDPOINT"),
        Bucket:   os.Getenv("S3_BUCKET"),
        Region:   os.Getenv("AWS_REGION"),
    })
    if err != nil {
        log.Fatal(err)
    }

    index, err := zenbun.Open(store, "indexes/articles/")
    if err != nil {
        log.Fatal(err)
    }

    // 競合したら、同期元と索引の状態を読み直して最大 3 回試す。
    err = zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
        return index.Sync(ctx, store, "articles/")
    })
    if err != nil {
        log.Fatal(err)
    }

    keys, err := index.Search(ctx, "日本語")
    if err != nil {
        log.Fatal(err)
    }
    for _, key := range keys {
        fmt.Println(key)
    }
}
```

`Sync` は文書の一覧と本文を別々に読みます。一覧の途中も含め、同期元の文書が同期中に変更されないよう、呼び出し側で調整してください。一覧と本文取得の間に ETag の変化を検出した場合は `ErrConflict` を返しますが、同期元全体を一瞬で撮影したような一覧は保証されません。

`NewS3Store` は、上記の環境変数または `S3Config` で渡した静的認証情報を使います。IAM ロールや IMDS による認証情報の取得には対応していません。パス形式を標準で使い、`S3Config.VirtualHost` でバーチャルホスト形式に切り替えられます。標準の HTTP タイムアウトは 30 秒です。独自の HTTP クライアントを渡す場合は、そのクライアント側でタイムアウトを設定してください。

## 候補を本文で確認する

`Search` はキーの一覧を返します。正確な結果が必要なら、各キーの元の本文を読み、**本文とクエリの両方**を同じ方法で正規化してから部分文字列を確認します。

```go
import (
    "strings"

    "golang.org/x/text/unicode/norm"
)

func containsQuery(body, query string) bool {
    normalize := func(s string) string {
        return strings.ToLower(norm.NFKC.String(s))
    }
    return strings.Contains(normalize(body), normalize(query))
}
```

これは確認部分の例です。実際には、返された各キーを使って元のストレージから本文を取得してから `containsQuery` に渡してください。文書が索引の作成後に変わっている場合は、取得した時点の本文で確認されます。

## コマンドラインで使う

このリポジトリのチェックアウトで、Go を使ってコマンドをインストールできます。

```sh
go install ./cmd/zenbun
```

`zenbun` が見つからない場合は、Go のインストール先（通常は `$(go env GOPATH)/bin`）に PATH が通っているか確認してください。上の環境変数を設定したうえで、次のように操作します。

```sh
# articles/ 以下の文書を索引に反映する
zenbun sync -source articles/ -index indexes/articles/

# 候補キーを 1 行ずつ表示する
zenbun search -index indexes/articles/ '日本語'

# 小さな索引をまとめ、不要になったデータを回収する
zenbun compact -index indexes/articles/
zenbun reclaim -index indexes/articles/ -grace 24h
```

`24h` は設定例です。安全な時間は環境によって異なります。次の「索引を手入れする」を読んで決めてください。

各コマンドで `-endpoint`、`-bucket`、`-region`、`-virtual-host` も指定できます。検索結果は 1 行 1 キーなので、キーに改行を含める場合は区別しやすい Go API を使ってください。

## 文書を個別に追加・更新・削除する

Go API の `Apply` は、複数の変更をまとめて公開できます。同じキーに本文を指定すると追加または置き換えになり、`Delete: true` なら削除になります。

```go
err := index.Apply(ctx, []zenbun.Change{
    {Key: "articles/a.txt", Data: []byte("新しい本文")},
    {Key: "articles/b.txt", Delete: true},
})
```

上のコードは、前の例で作った `ctx` と `index` を使う抜粋です。同じバッチに同じキーを 2 回含めることはできません。元の文書の変更は、別途呼び出し側で行ってください。

複数の書き手が同時に索引を更新した場合、一方の変更を黙って上書きせず、競合を `ErrConflict` として返します。`RetryConflict` を使うと再試行できます。再試行時は古い状態をそのまま使わず、`Sync` の例のように必要な状態を読み直してください。競合の調停や書き手間のロックは提供しません。

## 索引を手入れする

更新を重ねると、索引の小さな部品（セグメント）が増えます。部品が多いと検索時の読み取りや管理情報が増えるため、運用中は次の 2 つを定期的に実行してください。

1. **コンパクション**（`Compact` / `zenbun compact`）：複数のセグメントをまとめます。Go API の 1 回の呼び出しは最大 1 組、CLI はまとめられる組がなくなるまで繰り返します。
2. **回収**（`Reclaim` / `zenbun reclaim`）：参照されなくなったセグメントに印を付け、猶予時間が過ぎた後の実行で削除します。最初の実行ですべてが削除されるわけではありません。

回収の猶予時間は、**検索やコンパクションなどが索引を読み続ける最大時間・マシン間の時計差・ストレージ一覧の遅延の合計より長く**してください。すべての回収実行者が、この前提を守る必要があります。上限を保証できない環境では、安全に回収できません。

## 制限と検証状況

- 空のクエリや UTF-8 でない文書・キーは使えません。形態素解析、関連度順のランキング、AND 以外の演算子、本文を読む最終確認は提供しません。
- マニフェスト（有効なセグメントの一覧）、辞書ブロック、辞書目次、各エントリーの上限はそれぞれ 64 MiB です。上限を超える書き込みは公開前にエラーになります。
- マニフェストは文書一覧ではなくセグメント一覧を持ち、その大きさはセグメント数に比例します。通常の 1 件更新ではセグメントとマニフェストの 2 回の PUT を行います。
- CI では、Wikipedia 10,000 文書と青空文庫 2 作品を使い、178 クエリの候補を全文走査と比較しています。100,002 文書での転送量と取りこぼしは未検証です。大規模計測は 10 分以内に完了せず中止しました。CI の各ジョブには 10 分の上限があります。
- 読み取りは一時的なエラーに対して指数バックオフで再試行します。書き込み・削除は、結果が不明なときの競合を避けるため自動再試行しません。

保存形式の細部は[仕様書](docs/format.md)を参照してください。マニフェストとセグメントには形式バージョンがあり、知らないバージョンは読み込みません。

## ライセンス

[MIT License](LICENSE)（Copyright (c) 2026 s4na）
