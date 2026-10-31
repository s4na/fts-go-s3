# ストレージ互換性

必要な機能は Range GET、プレフィックスによる一覧、原子的な条件付き PUT、Delete と強い読み取り・一覧整合性です。対応の根拠は CI の `Storage compatibility` ワークフローです。各アダプターは共通の契約テスト `storetest.Run` を通しています。

| ストレージ | CI での検証 |
|---|---|
| VersityGW | Range、一覧、条件付き作成・更新の競合、同期と検索 |
| SeaweedFS | Range、一覧、条件付き作成・更新の競合、同期と検索 |
| Amazon S3 | 未検証。互換 API と条件付き書き込みの動作を利用者側で検証してください |
| fake-gcs-server（`gcsstore`） | Range、一覧、条件付き作成・更新の競合、同期と検索 |
| Google Cloud Storage | 未検証。`gcsstore` は JSON API と generation による条件付き書き込みを使います |

CI は [VersityGW](../.github/ci/versity/Dockerfile) と [SeaweedFS](../.github/ci/seaweedfs/Dockerfile) の Dockerfile でバージョンとダイジェストを固定して試験します。fake-gcs-server は [ワークフロー](../.github/workflows/e2e.yml) で Go モジュールのバージョンを固定し、チェックサムデータベースで検証して起動します。Dependabot は公開から 7 日の待機後に更新します。検証したバージョン以外の互換性を保証するものではありません。署名 v4、パス形式とバーチャルホスト形式を実装していますが、CI の互換ストレージ試験はパス形式です。

## Store の契約と使う機能

`zenbun.Store` を実装するストレージは、範囲指定を含む GET、原子的な条件付き PUT、DELETE、プレフィックスによる一覧を提供する必要があります。現在のインターフェースでは、メソッドはすべて必須です。ETag は不透明な値として維持し、一覧はキー順で返してください。読み取りと一覧には強い整合性が必要です。

機能ごとに見ると、検索は Range GET、更新は条件付き PUT、`Sync` は同期元の一覧と ETag、`Reclaim` は一覧・サーバーの LastModified・DELETE を使います。`Sync` と `Reclaim` を呼ばない使い方ではそれらの操作は実行されませんが、現在の `Store` インターフェースからメソッドを省くことはできません。`Reclaim` の猶予を判断するには、ストレージの時刻と実行環境の時刻が適切に同期されている必要があります。

## 設計の選択

| 案 | 効果 | 互換性と実装量 | 判断 |
|---|---|---|---|
| S3 クライアントをサブパッケージに分離 | コアから HTTP・XML・署名の直接依存を除く | `NewS3Store` と `S3Config` の import 変更が必要。索引形式は変えない | 採用 |
| List と LastModified を任意化 | 機能を絞ったストレージも利用しやすい | 回収・同期の設計と保存形式の変更が必要。未参照オブジェクトの回収にも課題 | 保存形式を検討する #33 と合わせて今後検討 |
| SDK アダプター | IAM ロールなど既存 SDK の認証を利用できる | コアに依存を追加しない別モジュールなどが必要 | 必要な SDK と運用条件が決まってから検討 |
| 自前 S3 機能の削減 | 保守対象を小さくできる可能性 | 既存利用者のアドレス形式や再試行に影響 | 互換性を維持し、今回は削除しない |
| FileStore | S3 なしで永続ストレージを試せる | プロセス間の条件付き書き込みや障害時の原子性を検証する必要 | 別途設計・実装を検討 |

移行する Go コードでは `github.com/s4na/fts-go-s3/s3store` を import し、`zenbun.NewS3Store(zenbun.S3Config{...})` を `s3store.NewS3Store(s3store.S3Config{...})` に置き換えてください。`Store` や索引の API と保存済みデータの形式は変わりません。
