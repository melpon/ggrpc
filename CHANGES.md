# 変更履歴

- UPDATE
    - 下位互換がある変更
- ADD
    - 下位互換がある追加
- CHANGE
    - 下位互換のない変更
- FIX
    - バグ修正

## master

- [ADD] `grpc::ClientContext` を取得する関数を追加

## 0.5.2 (2020-10-23)

- [ADD] `ClientManager::NotifyOnStateChange` を実装
- [ADD] やっぱりタイムアウト必要だったので復活

## 0.5.1 (2020-10-22)

- [CHANGE] タイムアウトは deadline で代用可能だったので削除
- [UPDATE] 依存ライブラリのバージョンアップ
- [FIX] サーバが特定の条件で正しく終了できないのを修正

## 0.5.0 (2020-10-21)

- [ADD] `ClientResponseReader` にタイムアウトを実装
- [FIX] CANCELING 中に Close すると間違った状態になるのを修正
- [FIX] clang だとオブジェクトが正しく解放されないのを修正

## 0.4.0 (2020-06-21)

- [CHANGE] ディレクトリ構成を変更して、`<ggrpc/ggrpc.h>` をインクルードして使うようにした
- [CHANGE] クライアントの `OnReadDone`, `OnResponse` を `OnFinish` に変更
- [CHANGE] `ClientResponseReader` の `Request` を `Connect` に変更
- [ADD] クライアントストリーミングを実装

## 0.3.0

- [ADD] サーバストリーミングを実装

## 0.2.3

- [FIX] ムーブ済みの値を使っていたのを修正

## 0.2.2

- [FIX] IDLE 状態の Write や WritesDone でセグフォしていたのを修正

## 0.2.1

- [FIX] 一部のコールバックが正しく動いてなかったのを修正

## 0.2.0

- [ADD] 書き込み成功のコールバックを実装
- [ADD] アラーム機能を追加

## 0.1.1

- [FIX] Server のリクエスト一覧が Collect() されてなかったのを修正

## 0.1.0

- [ADD] とりあえずリリース
