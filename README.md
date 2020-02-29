
## ルール

- ClientManager が死んだら ClientManager で生成した接続は全て安全に切断される
- ClientManager が死んだ後に ClientManager で生成したオブジェクトにアクセスするのは未定義
- Server が死んだら Server に紐付けられていたハンドラは全て安全に切断される
- Server が死んだ後に Server に紐付けられていたハンドラにアクセスするのは未定義
