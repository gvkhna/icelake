package flush

import (
	"bytes"
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// encodeParquet writes one batch as a single ZSTD-compressed Parquet file and
// returns its bytes.
//
// The whole file is built in memory, which is bounded by the same threshold
// that decided the batch was full — a batch is at most FlushMaxBytes of
// canonical payload, and the encoded file is smaller than that, not larger.
//
// The compression level is configuration rather than a constant so the same
// code path runs cheaply in tests and at a high ratio in production. Nothing
// about the file's identity depends on it: the object's name is a content hash
// over the batch's canonical *pre-encoding* form, so the same records under a
// different level are the same object rewritten, not a second one.
//
// The schema written is the file schema, whose field metadata carries each
// column's permanent field id, so a reader maps columns by the same ids the
// table does rather than by position or name.
func encodeParquet(record arrow.RecordBatch, schema *arrow.Schema, zstdLevel int) ([]byte, error) {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(zstdLevel),
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.NewArrowWriterProperties())
	if err != nil {
		return nil, err
	}
	if err := writer.Write(record); err != nil {
		// The writer owns an open row group at this point; closing it is the
		// only way to release what it holds, and its own error is the less
		// interesting of the two.
		_ = writer.Close()

		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// PutObject uploads one object.
//
// It is a plain PutObject rather than a multipart upload because a batch is
// bounded well below the 5 GiB single-request limit by the byte threshold that
// sealed it, and because a single request is what makes a retry a byte-for-byte
// overwrite of the same key.
//
// It is exported for the transition drain, which uploads spool files this
// package never sealed and must upload them exactly the way a flush would.
func PutObject(ctx context.Context, client *s3.Client, bucket, key string, body []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})

	return err
}
