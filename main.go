package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type CommandOption struct {
	Bucket string
	Regex  string
	Output bool
	Tags   string
}

type ListObjectsResponse struct {
	Keys      *[]*string
	NextToken *string
}

func s3ListObjects(client *s3.Client, bucket string, continueToken *string) (*ListObjectsResponse, error) {
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		MaxKeys:           1000,
		ContinuationToken: continueToken,
	})

	if err != nil {
		slog.Error("ListObjectV2エラー", "error", err.Error())
		return nil, err
	}

	var returnData []*string
	for _, c := range output.Contents {
		returnData = append(returnData, c.Key)
	}

	return &ListObjectsResponse{
		Keys:      &returnData,
		NextToken: output.NextContinuationToken,
	}, nil
}

func extractionOldFormat(s3Objects *[]*string, sRegex string) (*[]*string, error) {
	re, err := regexp.Compile(sRegex)
	if err != nil {
		slog.Error("正規表現コンパイルエラー", "error", err.Error())
		return nil, err
	}

	var oldFormatObjects []*string
	for _, d := range *s3Objects {
		if re.MatchString(*d) {
			oldFormatObjects = append(oldFormatObjects, d)
		}
	}

	return &oldFormatObjects, nil
}

func outputTaggingObjects(s3Objects *[]*string, fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		slog.Error("ファイル作成エラー", "error", err.Error())
		return err
	}

	writestr := ""
	for _, d := range *s3Objects {
		writestr += *d + "\n"
	}
	writeb := []byte(writestr)

	count, err := f.Write(writeb)
	if err != nil {
		slog.Error("ファイル書き込みエラー", "error", err.Error())
		return err
	}
	slog.Info("write" + strconv.Itoa(count) + "bytes")

	return nil
}

func s3PutTagging(client *s3.Client, bucket string, key string, tags []types.Tag) error {
	resp, err := client.PutObjectTagging(
		context.TODO(),
		&s3.PutObjectTaggingInput{
			Bucket: &bucket,
			Key:    &key,
			Tagging: &types.Tagging{
				TagSet: tags,
			},
		},
	)

	if err != nil {
		slog.Error("PutObjectTaggingエラー", "error", err.Error())
		return err
	}

	slog.Info("タグ付与完了", "bucket", bucket, "key", key, "metadata", resp.ResultMetadata)

	return nil
}

func generateTags(commandTags string) ([]types.Tag, error) {
	strTags := strings.Split(commandTags, ",")
	var tags []types.Tag
	for _, t := range strTags {
		keyval := strings.Split(t, "=")
		if len(keyval) != 2 {
			slog.Error("Tagのオプション引数が無効です", "tags", commandTags)
			return nil, errors.New("tag option error")
		}

		tags = append(tags, types.Tag{
			Key:   &keyval[0],
			Value: &keyval[1],
		})
	}

	return tags, nil
}

func main() {

	// コマンド引数の確認
	var commandOptions = [3]string{"bucket", "regex", "tags"}
	var commandValues [3]*string
	for i, d := range commandOptions {
		commandValues[i] = flag.String(d, "", d)
	}
	flag.Parse()

	for _, d := range commandValues {
		if len(*d) == 0 {
			slog.Error("コマンド引数エラー。bucket, regex, tagsの引数が必須です")
			return
		}
	}
	tags, err := generateTags(*commandValues[2])
	if err != nil {
		slog.Error("タグパースエラー")
		return
	}

	// AWSセットアップ
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWSの configロードエラー")
		return
	}
	client := s3.NewFromConfig(cfg)

	// バケットの全てのオブジェクトを取得
	var (
		token      *string
		allObjects []*string
	)
	for {
		resp, err := s3ListObjects(client, *commandValues[0], token)
		if err != nil {
			slog.Error("ListObjectsでエラー発生のため処理終了")
			return
		}

		allObjects = append(allObjects, *resp.Keys...)
		token = resp.NextToken

		if token == nil {
			break
		}
	}
	slog.Info("S3オブジェクト一覧の取得終了", "counts", len(allObjects))

	// 正規表現にマッチするオブジェクトの一覧を取得
	taggingObjects, err := extractionOldFormat(&allObjects, *commandValues[1])

	// タグ付けする対象の一覧を出力
	err = outputTaggingObjects(taggingObjects, "tagging-objects.csv")

	// タグ付け
	for _, o := range *taggingObjects {
		err = s3PutTagging(client, *commandValues[0], *o, tags)
		if err != nil {
			slog.Error("タグ付与失敗のため処理終了")
			return
		}
		time.Sleep(1 * time.Second)
	}

	slog.Info("タグ付け完了しました")
}
