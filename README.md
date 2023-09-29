# s3tagging
ローカル実行用S3オブジェクトタグ付けプログラム
## 使用方法
### コマンド
`go main.go -bucket "XXX" -regex "XX" -tags "Key=Value,Key=Value"`
### 引数
| 引数名 | 内容 | 例 |
| ---- | ---- | ---- |
| bucket | 対象のバケット名 | bucketname |
| regex | タグ付けする対象のオブジェクトキーの正規表現 | ^[0-9]. |
| tags | タグ内容 | test1key=test1value,test2key=test2value |