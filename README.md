# zenbun

**S3 互換ストレージや Google Cloud Storage に置いた UTF-8 テキストを、同じバケット上の索引で部分文字列検索する Go ライブラリと CLI です。**

[![CI](https://github.com/s4na/zenbun/actions/workflows/ci.yml/badge.svg)](https://github.com/s4na/zenbun/actions/workflows/ci.yml)
[![Storage compatibility](https://github.com/s4na/zenbun/actions/workflows/e2e.yml/badge.svg)](https://github.com/s4na/zenbun/actions/workflows/e2e.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

- **サーバー不要**：索引もバケットに置きます。検索時は索引の必要な部分だけを Range GET で読みます。
- **日本語をそのまま検索**：形態素解析は使いません。NFKC 正規化と文字 bi-gram により、任意の部分文字列を探せます。
- **差分更新**：追加・更新・削除を、索引を作り直さずに反映します。公開は条件付き PUT で原子的に行います。
- **取りこぼしを CI で確認**：10,002 文書・178 クエリで全文走査と照合しています。検索の転送量は全文走査の約 1/1,380 です（[計測](#検証状況と性能)）。

> [!IMPORTANT]
> `Index.Search` が返すのは、検索文字列を**含む可能性がある**文書のキーです。正確な一致が必要なら、[本文で確認する](#候補を本文で確認する)か、本文のコピーごと管理する [`Mirror`](#本文のコピーも管理する-mirror) を使ってください。

> [!NOTE]
> v1.0 より前の段階です。API と保存形式は変わる可能性があります。

## クイックスタート

Go 1.27 以上が必要です。

旧モジュール `github.com/s4na/fts-go-s3` を利用している場合は、依存関係とすべての import パスを `github.com/s4na/zenbun` に変更してください。旧パスと新パスは Go では別のモジュールとして扱われます。

```sh
go get github.com/s4na/zenbun
```

S3 を用意しなくても、メモリストアで試せます。

```go
package main

import (
    "context"
    "fmt"
    "log"

    zenbun "github.com/s4na/zenbun"
)

func main() {
    ctx := context.Background()

    index, err := zenbun.Open(&zenbun.MemoryStore{}, "indexes/articles/")
    if err != nil {
        log.Fatal(err)
    }

    // 文書のキーと本文を索引に登録する（本文の保存はしない）。
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

## S3 互換ストレージで使う

`articles/` 以下の文書を `indexes/articles/` の索引に同期して検索します。`Sync` は一覧の ETag を比べ、追加・更新・削除された文書だけを反映します。

接続先と認証情報は `S3Config` で渡します。AWS 以外の S3 互換ストレージ（MinIO、Cloudflare R2、VersityGW、SeaweedFS など）でも同じです。

```go
import (
    zenbun "github.com/s4na/zenbun"
    "github.com/s4na/zenbun/s3store"
)

store, err := s3store.NewS3Store(s3store.S3Config{
    Endpoint:  "https://s3.example.com", // 必須。AWS 以外ならそのサービスの URL
    Bucket:    "example-bucket",
    Region:    "us-east-1", // 署名に使う。省略時は us-east-1
    AccessKey: accessKey,   // シークレット管理などから読み込んだ値
    SecretKey: secretKey,
    // AccessKey と SecretKey を両方省略すると、
    // 環境変数 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN を読む。
})
if err != nil {
    log.Fatal(err)
}

index, err := zenbun.Open(store, "indexes/articles/")
if err != nil {
    log.Fatal(err)
}

// ほかの書き手と競合したら、状態を読み直して最大 3 回試す。
err = zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
    return index.Sync(ctx, store, "articles/")
})
if err != nil {
    log.Fatal(err)
}

keys, err := index.Search(ctx, "日本語")
```

同じことを CLI でも行えます。CLI は接続先と認証情報を環境変数から読みます。`AWS_` で始まる名前は、AWS 以外の S3 互換ストレージでも aws CLI などと共通の慣習的な名前です。

```sh
export S3_ENDPOINT='https://s3.example.com'
export S3_BUCKET='example-bucket'
export AWS_REGION='us-east-1'           # 省略時は us-east-1
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'

go install github.com/s4na/zenbun/cmd/zenbun@latest

zenbun sync    -source articles/ -index indexes/articles/
zenbun search  -index indexes/articles/ '日本語'
zenbun compact -index indexes/articles/
```

本番で使う前に、少なくとも[はじめに守ること](#はじめに守ること)と[索引の手入れ](#索引の手入れ)を読んでください。wiki などの既存のアプリに組み込む場合は、[アプリに組み込む](docs/integration.md)も参照してください。

## 目次

- [はじめに守ること](#はじめに守ること)
- [仕組み](#仕組み)
- [どの API を使うか](#どの-api-を使うか)
- [索引を更新する](#索引を更新する)
- [本文のコピーも管理する Mirror](#本文のコピーも管理する-mirror)
- [候補を本文で確認する](#候補を本文で確認する)
- [条件を組み合わせて検索する Find](#条件を組み合わせて検索する-find)
- [行範囲を指定して読む Read](#行範囲を指定して読む-read)
- [同時更新と競合](#同時更新と競合)
- [索引の手入れ](#索引の手入れ)
- [ストレージアダプター](#ストレージアダプター)
- [S3 の設定](#s3-の設定)
- [GCS の設定](#gcs-の設定)
- [コマンドライン](#コマンドライン)
- [制限](#制限)
- [検証状況と性能](#検証状況と性能)
- [保存形式と互換性](#保存形式と互換性)
- [開発](#開発)

## はじめに守ること

- **文書は UTF-8 のテキスト**にしてください。画像や PDF からのテキスト抽出は行いません。
- **文書と索引のプレフィックスを重ねないでください。** S3 の一覧は文字列の前方一致なので、`doc` と `docs-index/` は重なります。`doc/` のように末尾の `/` まで指定してください。
- **1 つの索引は 1 つの同期元専用にしてください。** `Sync` は同期元の一覧にないキーを索引から削除します。別の同期元や `Apply` で登録したキーを混ぜると、それらも消えます。
- **索引は自動では更新されません。** 文書を変更したら `Sync` か `Apply` を呼んでください。反映前は、新しい文書が見つからず、削除した文書が候補に残ります。
- **定期的にコンパクションと回収を実行してください。** 詳しくは[索引の手入れ](#索引の手入れ)を参照してください。

## 仕組み

```text
バケット
├── articles/              元の文書（zenbun は読むだけ）
│   ├── a.txt              「日本語の文書」
│   └── b.txt              「英語の文書」
└── indexes/articles/      zenbun が管理する索引
    ├── manifest           現在有効なセグメントの一覧
    └── segments/...       不変のセグメント
```

```mermaid
flowchart TB
    U1(["更新"]) --> U2["Sync / Apply"]
    U2 --> U3["新しいセグメントを保存"]
    U3 --> U4["manifest を条件付き PUT"]
    S1(["検索"]) --> S2["manifest を読む"]
    S2 --> S3["必要な部分だけ Range GET"]
    S3 --> S4["候補キー"]
    S4 -. 必要なら .-> S5["本文で確認"]
```

文書と検索文字列は、**NFKC 正規化**のあと小文字化します。たとえば `ＡＢＣ` と `abc` は同じ扱いです。そのうえで 2 文字ずつの組（**文字 bi-gram**）に分けて索引に記録します。1 文字の検索には 1 文字単位の記録（uni-gram）を使います。

文字の位置は記録しないため、候補には一致しない文書が混ざることがあります。たとえば `猫犬猫` は `猫犬` と `犬猫` に分かれるので、`猫犬と犬猫` も候補になります。

```mermaid
flowchart TB
    Q["検索: 猫犬猫"] --> B1["猫犬"]
    Q --> B2["犬猫"]
    B1 --> C{"両方の組を持つ文書"}
    B2 --> C
    C --> D1["猫犬猫です<br>（本当に一致）"]
    C --> D2["猫犬と犬猫<br>（組はあるが一致しない）"]
```

検索結果はキー順で、関連度による順位はありません。

## どの API を使うか

| やりたいこと | 使う API |
|---|---|
| S3 のプレフィックス以下の文書をまとめて索引にする | `Index.Sync` |
| アプリから変更を受け取って、キーと本文で索引を更新する | `Index.Apply` |
| Firebase などの外部データの本文を S3 にコピーし、索引も一緒に更新する | `Mirror.Apply` |
| 候補キーだけ欲しい（本文の確認は自分でする） | `Index.Search` |
| 本文を確認済みの結果が欲しい | `Mirror.Search` / `Mirror.SearchLimit` |
| 複数の語を AND・OR・NOT で組み合わせ、一致した行だけ欲しい（LLM のツールなど） | `Index.Find` / `Mirror.Find` |
| 統合・不要データの削除 | `Index.Compact` / `Index.Reclaim` |

## 索引を更新する

### Sync：S3 上の文書と同期する

```go
err := zenbun.RetryConflict(ctx, 3, func(ctx context.Context) error {
    return index.Sync(ctx, store, "articles/")
})
```

- 変更は最大 1,000 文書ずつ公開します。途中でエラーになった場合、それまでのバッチは反映済みです。状態を読み直して再実行すると、続きから同期します。
- 一覧と本文の読み取りは別々に行います。同期中は同期元の文書を変更しないよう、呼び出し側で調整してください。一覧の後に ETag が変わったことを検出した場合は `ErrConflict` を返します。ただし、同期元全体の一覧が一瞬の状態を表すことは保証しません。
- 大量に同期するとセグメントが増え、検索で読む量が増えます。同期後は `zenbun compact` を実行するか、`Compact` を `false` が返るまで繰り返してください。

### Apply：変更を直接渡す

```go
err := index.Apply(ctx, []zenbun.Change{
    {Key: "articles/a.txt", Data: []byte("新しい本文")}, // 追加または置き換え
    {Key: "articles/b.txt", Delete: true},              // 削除
})
```

`Apply` はバッチ全体を 1 回で公開します。同じキーを 1 つのバッチに 2 回含めることはできません。**`Apply` は索引を更新するだけで、元の文書を保存・削除しません。**

## 本文のコピーも管理する Mirror

元データが S3 以外（Firebase など）にある場合、`Mirror` を使うと、本文の S3 へのコピーと索引の更新をまとめて行えます。検索時はコピーした本文で一致を確認し、本文付きの結果を返します。元データの取得や変更通知の受け取りはアプリ側で行います。

```go
mirror, err := zenbun.NewMirror(index, store, "copies/articles/")
if err != nil {
    log.Fatal(err)
}

// 元データの追加・更新・削除を受け取ったら渡す。
err = mirror.Apply(ctx, []zenbun.Change{
    {Key: "article-1", Data: []byte("日本語の記事です")},
    {Key: "article-2", Delete: true},
})
if err != nil {
    log.Fatal(err) // 変更を保持したまま、同じバッチを再試行する。
}

// 本文で一致を確認した結果を最大 20 件返す。
results, err := mirror.SearchLimit(ctx, "日本語", 20)
for _, doc := range results {
    fmt.Printf("%s: %s\n", doc.Key, doc.Data)
}
```

- 文書 ID には `user/123` のように `/` を含められます。ただし、空・`.`・`..` の要素は使えません。
- コピーと索引のプレフィックスを重ねないでください。また、`Mirror` で使う索引に `Index.Apply` や `Index.Sync` を直接混ぜないでください。
- **コピーの更新と索引の公開は 1 つのトランザクションではありません。** `Mirror.Apply` は次の順に処理します。

  ```mermaid
  sequenceDiagram
      participant App as アプリ
      participant M as Mirror
      participant C as コピー（copies/）
      participant I as 索引
      App->>M: Apply(変更のバッチ)
      M->>C: 追加・更新分を PUT
      M->>I: 索引を公開（manifest を条件付き PUT）
      M->>C: 削除分を DELETE
      M-->>App: 成功 / エラー
      Note over App,I: どこで失敗しても、同じバッチを後続より先に再試行する
  ```

  途中で失敗すると、コピーだけが新しい状態、または削除済みのはずのコピーが残る状態になることがあります。変更通知は永続的に保持してください。同じ `Mirror` への書き込みは全プロセスで直列化し、失敗したバッチは後続の変更より先に再試行してください。`Mirror` 自体は通知の保存・順序制御・自動再試行を行いません。
- `Mirror.Search` は候補ごとにコピーの本文を 1 回 GET します。短い検索語や頻出語では、ほぼ全文書を読むことがあります。`SearchLimit` は指定件数に一致したところで本文の取得を止めますが、索引の候補はすべて取得します。続きのページを取得する機能はありません。
- コピーが見つからない場合は、空の結果ではなくエラーを返します。

## 候補を本文で確認する

`Index.Search` の候補から正確な結果を得るには、本文と検索文字列を同じ方法で正規化し、部分一致を確認します。

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

これは**正規化後の一致**の確認です。文字幅や大文字・小文字まで区別したい場合は、元の本文と検索文字列で追加の確認をしてください。本文の確認では、新規追加・更新に伴う取りこぼしは補えません。先に索引へ変更を反映してください。

## 条件を組み合わせて検索する Find

`Find` は、複数の語の組み合わせを 1 回で検索し、本文で一致を確かめた結果を返します。返す量も指定できます。LLM がツールとして検索するとき、何度も検索して結果を突き合わせずに済むので、コンテキストを節約できます。

```go
query := zenbun.Query{
    All:       [][]string{{"猫", "ネコ", "cat"}, {"請求書", "インボイス"}},
    None:      []string{"下書き"},
    KeyPrefix: "articles/2026/",
}
result, err := index.Find(ctx, store, query, zenbun.FindOptions{Context: 1})
if err != nil {
    log.Fatal(err)
}
for _, doc := range result.Documents {
    for _, s := range doc.Snippets {
        fmt.Printf("%s:%d\n%s\n", doc.Key, s.Line, s.Text)
    }
}
if result.Truncated {
    // 続きは FindOptions{After: result.Next} で取得する。
}
```

- **`All`**：内側の配列が 1 つの概念を表し、その中の語はどれか 1 つを含めば一致します（OR）。概念どうしは、すべてを含む必要があります（AND）。言い換えや表記ゆれは、同じ概念の中に並べてください。
- **`None`**：このどれかを含む文書は除きます。索引では除外できないため、本文を確認するときに除きます。
- **`KeyPrefix`**：キーの前方一致で絞り込みます。`Mirror.Find` では文書 ID に対して効きます。
- 語の一致は `Search` と同じく、NFKC 正規化と小文字化のあとの部分一致です。

| `FindOptions` | 内容 | 既定値 |
|---|---|---|
| `Mode` | `snippets`（一致した行と前後の行）・`keys`（キーだけ）・`count`（件数だけ） | `snippets` |
| `Limit` | 本文で一致を確認できた文書がこの数に達したら止める | 20 |
| `Context` | 一致した行の前後に付ける行数（最大 20） | 0 |
| `MaxSnippetsPerDoc` | 1 文書あたりの抜粋の数 | 3 |
| `MaxBytes` | 抜粋の合計バイト数。超えた分は文字の途中で切らずに打ち切る | 16 KiB |
| `After` | このキーより後から検索する。前回の結果の `Next` を渡す | なし |
| `Concurrency` | 本文を同時に読む数（最大 64）。JSON では指定できない | 4 |
| `Matches` | 抜粋の中で語に一致した範囲（`Snippet.Matches`）も返す | `false` |

- 結果は**キー順**で、関連度による順位はありません。
- `Candidates` は、索引で絞った候補の数です。本文で確認する前の上限の目安になります。
- `Limit` か `MaxBytes` の上限で止まった場合、`Truncated` が `true` になり、`Next` に続きの開始位置が入ります。
- `Lines` は、その文書の中で語を含む行の数です。`MaxSnippetsPerDoc` で抜粋を省いても検索は止まらないので、`Truncated` は変わりません。`Lines` と抜粋の数を比べると、省いた行があるか分かります。
- `Index.Find` は、`Sync` で同期した同期元の本文を読みます。同期後に削除された文書は結果から外し、エラーにはしません。`Mirror.Find` は、コピーが見つからない場合にエラーを返します。
- 候補 1 件ごとに本文を 1 回 GET します。`Limit` 件の一致が見つかった時点で読むのを止めますが、`None` で多くの候補が外れる検索では、多くの本文を読むことがあります。
- 本文は `Concurrency` 件まで同時に読みます。結果はキー順のままです。止まった時点で先読み中の読み取りは取り消すので、余分な GET は最大 `Concurrency - 1` 回です。ストレージへの負荷を抑えたい場合は小さく、遅延が大きいストレージでは大きくしてください。
- 1 回の検索で使える語は合計 64 個まで、概念は 16 個までです。語の長さは 256 バイトまでです。抜粋は行単位なので、`All` の語には改行を含められません。
- 索引だけで候補キーが欲しい場合は `Index.SearchQuery` を使います（`None` は適用されません）。
- **`Matches`**：画面でハイライトする場合に使います。`Snippet.Matches` の `Start`・`End` は、正規化する前の `Text` 上のバイト位置です（`Text[Start:End]`）。全角の「ＡＢＣ」や半角の「ｶﾞｲﾄﾞ」に一致した場合も、元の文字の範囲を指します。正規化で長さが変わる文字（「㍿」→「株式会社」など）は、語が一部に一致した場合も、文字全体を範囲にします。範囲は先頭から順に並び、重なったり接したりする範囲は 1 つにまとめます。`MaxBytes` で切った抜粋では、切った後の `Text` に収まる範囲だけを返します。JavaScript で使う場合は、UTF-16 の位置に変換してください。

コマンドラインでは、クエリを JSON で渡し、結果を JSON で受け取ります。LLM のツールとして呼ぶ場合に便利です。未知のフィールドはエラーにします。

```sh
zenbun find -index indexes/articles/ -context 1 -limit 10 \
  '{"all":[["猫","ネコ"],["請求書"]],"none":["下書き"],"key_prefix":"articles/"}'
```

```json
{"count":1,"candidates":2,"documents":[{"key":"articles/a.txt","lines":1,"snippets":[{"line":2,"text":"前の行\n猫の請求書です\n次の行"}]}]}
```

## 行範囲を指定して読む Read

`Read` は、1 つの文書から行範囲を指定して読みます。`Find` の抜粋の前後をもう少し読みたいときや、`MaxSnippetsPerDoc` で省かれた行を読みたいときに、本文全体を取得せずに済みます。

```go
doc := result.Documents[0]
line := doc.Snippets[0].Line
r, err := index.Read(ctx, store, doc.Key, zenbun.ReadOptions{Start: line - 5, End: line + 20})
if err != nil {
    log.Fatal(err)
}
fmt.Println(r.Text)
if r.Next != 0 {
    // 続きは ReadOptions{Start: r.Next, End: ...} で読む。
}
```

| `ReadOptions` | 内容 | 既定値 |
|---|---|---|
| `Start` | 最初の行（1 始まり）。`Snippet.Line` と同じ番号 | 必須 |
| `End` | 最後の行。文書の行数を超えた分は無視する | 文書の最後 |
| `MaxBytes` | `Text` の最大バイト数。収まらない行から先は返さない | 16 KiB |

- 行の区切り方は `Find` の抜粋と同じです。`\n` で区切り、`\r` は除きます。`Lines` は文書全体の行数です。
- `MaxBytes` で切った場合は `Truncated` が `true` になり、`Next` に続きの開始行が入ります。最初の 1 行だけで `MaxBytes` を超える場合は、その行を文字の途中で切らない位置で打ち切り、`Next` は次の行になります（残りは読めません）。
- `Start` が 1 未満か文書の行数を超える場合、`End` が `Start` より前の場合はエラーです。文書がない場合は `ErrNotFound` を包んだエラーを返します。
- `Index.Read` は索引を確認せず、`source` の任意のキーを読みます。LLM などから受け取ったキーを渡す場合は、呼び出す前に閲覧権限を確認してください。`Mirror.Read` は文書 ID の形式（空・`.`・`..` の要素を含まない）を確認します。

コマンドラインでは、結果を JSON で表示します。

```sh
zenbun read -index indexes/articles/ -start 10 -end 30 articles/a.txt
```

```json
{"key":"articles/a.txt","start":10,"text":"…","lines":120}
```

## 同時更新と競合

索引の部品（セグメント）を先に保存し、最後に**マニフェスト**を条件付き PUT で差し替えます。

```mermaid
sequenceDiagram
    participant A as 書き手 A
    participant B as 書き手 B
    participant S as ストレージ
    A->>S: manifest を読む（ETag: e1）
    B->>S: manifest を読む（ETag: e1）
    A->>S: セグメントを保存
    B->>S: セグメントを保存
    A->>S: manifest を PUT（If-Match: e1）
    S-->>A: 成功（ETag: e2）
    B->>S: manifest を PUT（If-Match: e1）
    S-->>B: 412 → ErrConflict
    Note over B: 状態を読み直して再試行（RetryConflict）
```

- マニフェストの差し替え前に処理が止まった場合、読み手は更新前の索引を読み続けます。
- 複数の書き手が同じ状態から更新した場合、一方の変更を黙って上書きせず、`ErrConflict` を返します。`RetryConflict` で再試行できますが、再試行のたびに状態を読み直してください（上の `Sync` の例のように、関数の中で読み直す処理を呼びます）。
- 書き手間のロックや競合の調停は提供しません。
- マニフェストの書き込み中に通信が切れた場合、サーバー側では更新が完了している可能性があります。結果が不明なときは、索引の状態を確認してから再試行してください。
- 読み取りは一時的なエラーを指数バックオフで再試行します。書き込みと削除は、結果が不明なときの競合を避けるため自動では再試行しません。

## 索引の手入れ

更新を重ねるとセグメントが増え、検索で読む量とマニフェストが大きくなります。運用中は、次の 2 つを定期的に実行してください。

1. **コンパクション**（`Compact` / `zenbun compact`）：隣り合うセグメントをまとめます。Go API は 1 回の呼び出しで最大 1 組をまとめ、まとめた場合に `true` を返します。CLI は、まとめられる組がなくなるまで繰り返します。
2. **回収**（`Reclaim` / `zenbun reclaim`）：参照されなくなったセグメントに印を付け、猶予時間が過ぎた後の実行で削除します。1 回目の実行ではすべては消えません。

```mermaid
stateDiagram-v2
    direction TB
    live: 使用中（manifest が参照）
    orphan: 未参照
    retired: 印付き（retired/ に印）
    [*] --> live: 公開
    [*] --> orphan: 公開前に失敗
    live --> orphan: Compact で統合される
    orphan --> retired: Reclaim
    retired --> [*]: 猶予時間の経過後の Reclaim で削除
```

回収の猶予時間は、**読み取り操作の最大時間・マシン間の時計のずれ・ストレージの一覧の遅延の合計より長く**してください。読み取り操作の最大時間とは、検索・同期・コンパクションが索引を読み始めてから読み終えるまでの最長時間です。回収を実行するすべてのプロセスが、この前提を守る必要があります。上限を保証できない環境では、安全に回収できません。

```sh
zenbun reclaim -index indexes/articles/ -grace 24h
```

## ストレージアダプター

索引と検索の本体は特定のストレージに依存しません。読み書きはすべて `zenbun.Store` インターフェース（`Get` / `Put` / `Delete` / `List`）を通るので、アダプターを差し替えるだけで保存先を切り替えられます。

| アダプター | パッケージ | 用途 |
|---|---|---|
| `MemoryStore` | `zenbun` | テストや試用 |
| `S3Store` | `github.com/s4na/zenbun/s3store` | Amazon S3 と S3 互換ストレージ（MinIO、Cloudflare R2、VersityGW、SeaweedFS など） |
| `GCSStore` | `github.com/s4na/zenbun/gcsstore` | Google Cloud Storage（JSON API） |

```go
var store zenbun.Store
var err error
switch backend {
case "gcs":
    store, err = gcsstore.NewGCSStore(gcsstore.GCSConfig{Bucket: "example-bucket", HTTPClient: authenticatedClient})
default:
    store, err = s3store.NewS3Store(s3store.S3Config{Endpoint: "https://s3.example.com", Bucket: "example-bucket"})
}
index, err := zenbun.Open(store, "indexes/articles/")
```

ほかのストレージを使う場合は、`zenbun.Store` を実装してください。条件付き PUT が複数の書き手に対して原子的に働くこと、ETag（版の識別子）を `Get` / `Put` / `List` で同じ値として返すことが必要です。実装が契約を満たすかは、`storetest.Run` で確かめられます。同梱のアダプターも CI で同じテストを通しています。

```go
func TestMyStore(t *testing.T) {
    storetest.Run(t, newEmptyMyStore(t)) // 空のバケットや名前空間を渡す
}
```

## S3 の設定

- **認証**：静的なアクセスキーで署名（SigV4）します。匿名アクセス、IAM ロール、IMDS には対応していません。認証情報はリポジトリに書き込まないでください。認証情報は次の順で決まります。

  | 優先 | 取得元 | 条件 |
  |---|---|---|
  | 1 | `S3Config` の `AccessKey` / `SecretKey` / `SessionToken` | `AccessKey` と `SecretKey` の少なくとも一方を指定した場合（このとき環境変数は読まない） |
  | 2 | 環境変数 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | `AccessKey` と `SecretKey` を両方省略した場合 |

  最終的に `AccessKey` と `SecretKey` がそろわない場合、`NewS3Store` はエラーを返します。
- **リージョン**：S3 互換ストレージでも署名の計算に使います。省略時は `us-east-1` です。多くの互換ストレージはこの値のまま動きます。動かない場合は、サービスの指定に合わせてください。
- **アドレス形式**：標準ではパス形式を使います。`VirtualHost: true` でバーチャルホスト形式に切り替えられます。
- **HTTP**：標準のタイムアウトは 30 秒です。`HTTPClient` を渡す場合は、タイムアウトを自分で設定してください。検索は最大 8 並列で GET するため、`http.Transport` の `MaxIdleConnsPerHost` を 8 以上にすると接続を再利用しやすくなります（同時接続数の上限ではありません）。
- **必要な機能**：Range GET、一覧、削除、原子的な条件付き PUT、強い読み取り・一覧整合性が必要です。CI では VersityGW と SeaweedFS で検証しています。Amazon S3 自体はまだ検証していません。
- **詳細**：IAM 権限・プレフィックス・競合は [S3 運用上の前提](docs/storage-operations.md) を、`Store` の契約と対応ストレージは [ストレージ互換性](docs/compatibility.md) を参照してください。

## GCS の設定

```go
import (
    "golang.org/x/oauth2/google"
    "github.com/s4na/zenbun/gcsstore"
)

client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_write")
store, err := gcsstore.NewGCSStore(gcsstore.GCSConfig{Bucket: "example-bucket", HTTPClient: client})
```

- **認証**：認証済みの `HTTPClient`（上の例ではアプリケーションのデフォルト認証情報）を渡すと、トークンの更新もそのクライアントに任せます。`golang.org/x/oauth2` は zenbun の依存ではないので、使う側で追加してください。短時間の作業なら、`AccessToken` に OAuth 2.0 アクセストークンを直接渡すこともできます。どちらも指定しない場合は、環境変数 `GOOGLE_OAUTH_ACCESS_TOKEN` を読みます。
- **ETag**：GCS のオブジェクトの generation を ETag として使い、条件付き PUT は `ifGenerationMatch` で行います。内容が同じでも上書きすると generation は変わるため、`Sync` はその文書を読み直します。
- **接続先**：標準は `https://storage.googleapis.com` です。エミュレーターなどを使う場合は `Endpoint` を指定してください。
- **HTTP**：`HTTPClient` を省略した場合のタイムアウトは 30 秒です。
- **検証状況**：CI では [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) で検証しています。Google Cloud Storage 本体ではまだ検証していません。

## コマンドライン

```sh
go install github.com/s4na/zenbun/cmd/zenbun@latest
```

| コマンド | 内容 |
|---|---|
| `zenbun sync -source <prefix> -index <prefix>` | 同期元を索引に反映（競合時は最大 3 回再試行） |
| `zenbun search -index <prefix> <query>` | 候補キーを 1 行に 1 つ表示 |
| `zenbun find -index <prefix> [-mode -limit -context -max-snippets -max-bytes -after -matches] '<JSON>'` | 条件を組み合わせて検索し、本文で確認した結果を JSON で表示（[Find](#条件を組み合わせて検索する-find)） |
| `zenbun read -index <prefix> [-start -end -max-bytes] <key>` | 文書の行範囲を JSON で表示（[Read](#行範囲を指定して読む-read)） |
| `zenbun compact -index <prefix>` | まとめられるセグメントがなくなるまでコンパクション |
| `zenbun reclaim -index <prefix> -grace <期間>` | 不要なセグメントを回収（`-grace` は `24h` や `90m` の形式、既定値 `24h`） |

`-storage` で保存先のアダプターを `s3`（既定）か `gcs` から選びます。省略すると環境変数 `ZENBUN_STORAGE` の値を使います。

| オプション | `s3` で省略したとき | `gcs` で省略したとき |
|---|---|---|
| `-endpoint` | `S3_ENDPOINT` | `GCS_ENDPOINT`、なければ `https://storage.googleapis.com` |
| `-bucket` | `S3_BUCKET` | `GCS_BUCKET` |
| `-region` | `AWS_REGION` | 指定するとエラー |
| `-virtual-host` | パス形式 | 指定するとエラー |

認証情報はオプションでは渡せません（プロセス一覧に秘密情報が見えないようにするため）。環境変数で渡してください。

- **S3**：`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`（一時的な認証情報なら `AWS_SESSION_TOKEN` も）
- **GCS**：`GOOGLE_OAUTH_ACCESS_TOKEN` のアクセストークン（例：`export GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)"`）。トークンには有効期限（通常 1 時間）があります。

`zenbun` が見つからない場合は、`$(go env GOPATH)/bin` が PATH に含まれているか確認してください。キーに改行を含む場合は、出力を区別しやすい Go API を使ってください。

## 制限

- 空のクエリや、UTF-8 でない文書・キーは使えません。
- 形態素解析、関連度順のランキング、AND 以外の演算子、ページングは提供しません。
- マニフェスト、辞書ブロック、辞書目次、各エントリーの上限はそれぞれ 64 MiB です。上限を超える書き込みは、公開前にエラーになります。
- マニフェストのサイズはセグメント数に比例します（文書数ではありません）。1 件の更新では、セグメントとマニフェストの 2 回の PUT を行います。

## 検証状況と性能

CI の [Corpus oracle and measurements](.github/workflows/oracle.yml) は、Wikipedia 10,000 文書と青空文庫 2 作品に対して 178 クエリの結果を全文走査と比較します。この条件で取りこぼしが 0 件であることを確認しています。

メモリストアで 1,000 文書ごとにセグメントを作った計測（main、2026 年 10 月時点）の結果は次のとおりです。

| 指標 | 値 |
|---|---|
| 1 クエリあたりの GET | 平均 71.4 回 |
| 1 クエリあたりの受信量 | 平均 約 134 KB |
| 全文書を 1 回ずつ読む場合との比 | 約 1/1,380 |
| 1 件更新の PUT | 2 回 |

対象は索引検索の GET の応答本文です。HTTP ヘッダーや、候補の本文確認の読み取りは含みません。実ストレージでの検索遅延を表す数値でもありません。最新の値は CI の実行結果のサマリーに表示されます。

**100,002 文書での転送量と取りこぼしは未検証です。** 大規模な計測は CI のジョブの上限（10 分）以内に完了しませんでした。

## 保存形式と互換性

マニフェストとセグメントには形式バージョンがあり、知らないバージョンは読み込みません。新しいセグメントは**保存形式 v2** で書きます。v1 と v2 の両方を読めますが、v2 を書いた索引は古いライブラリでは読めません。同じ索引を複数のプロセスで使う場合は、書き込みを止めて全プロセスを更新してから再開してください。

正規化ライブラリの更新で正規化結果が変わった場合は、索引を元の本文から作り直す必要があります。詳しくは [保存形式の仕様](docs/format.md) を参照してください。

## 開発

CI は単体テスト、S3 互換ストレージと fake-gcs-server での E2E テスト、コーパスを使った照合と計測を実行します。

Dependabot は、Go モジュール・GitHub Actions・CI のストレージイメージの更新を毎週確認し、リリースから 7 日以上たった版を提案します。セキュリティ更新は GitHub の仕様により、この待機期間の対象外です。E2E テストで使う fake-gcs-server の版は `.github/workflows/e2e.yml` で固定しており、Dependabot の対象外です。GitHub Actions の依存は `.github/workflows/actions.lock` に記録しています。Action を追加・変更した場合は、`gh-actions-lock` 拡張で lockfile を更新してください。

## ライセンス

[MIT License](LICENSE)（Copyright (c) 2026 s4na）
