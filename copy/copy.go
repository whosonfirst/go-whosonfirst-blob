package copy

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"sync/atomic"

	aa_log "github.com/aaronland/go-log"
	"github.com/aaronland/gocloud-blob/bucket"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"gocloud.dev/blob"
)

type CopyStats struct {
	Seen    uint64
	Skipped uint64
	Copied  uint64
}

type CopyOptions struct {
	BucketURI       string
	IteratorURI     string
	IteratorSources []string
	Logger          *log.Logger
	ACL             string
	Verbose         bool
}

func Copy(ctx context.Context, opts *CopyOptions) (*CopyStats, error) {

	seen := uint64(0)
	skipped := uint64(0)
	copied := uint64(0)

	if opts.Verbose {
		aa_log.SetMinLevelWithPrefix(aa_log.DEBUG_PREFIX)
	} else {
		aa_log.SetMinLevelWithPrefix(aa_log.WARNING_PREFIX)
	}

	target_bucket, err := bucket.OpenBucket(ctx, opts.BucketURI)

	if err != nil {
		return nil, fmt.Errorf("Failed to open target bucket, %w", err)
	}

	defer target_bucket.Close()

	iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		atomic.AddUint64(&seen, 1)

		id, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return fmt.Errorf("Failed to parse %s, %w", path, err)
		}

		rel_path, err := uri.Id2RelPath(id, uri_args)

		if err != nil {
			return fmt.Errorf("Failed to derive relative path for %s, %w", path, err)
		}

		exists, err := target_bucket.Exists(ctx, rel_path)

		if err != nil {
			return fmt.Errorf("Failed to determine whether %s exists, %w", rel_path, err)
		}

		if exists {

			attrs, err := target_bucket.Attributes(ctx, rel_path)

			if err != nil {
				return fmt.Errorf("Failed to derive attributes for %s, %w", rel_path, err)
			}

			if attrs.MD5 != nil {

				h := md5.New()

				_, err := io.Copy(h, r)

				if err != nil {
					return fmt.Errorf("Failed to copy %s to MD5 hash, %w", rel_path, err)
				}

				md5 := h.Sum(nil)

				if bytes.Equal(attrs.MD5, md5) {
					aa_log.Debug(opts.Logger, "%s and %s are the same, skipping\n", path, rel_path)
					atomic.AddUint64(&skipped, 1)
					return nil
				}

				_, err = r.Seek(0, 0)

				if err != nil {
					return fmt.Errorf("Failed to rewind reader for %s, %w", path, err)
				}
			}
		}

		before := func(asFunc func(interface{}) bool) error {

			req := &s3manager.UploadInput{}
			ok := asFunc(&req)

			if !ok {
				return nil
			}

			req.ACL = aws.String(opts.ACL)
			return nil
		}

		wr_opts := &blob.WriterOptions{
			BeforeWrite: before,
		}

		wr, err := target_bucket.NewWriter(ctx, rel_path, wr_opts)

		if err != nil {
			return fmt.Errorf("Failed to create new writer for %s, %w", rel_path, err)
		}

		_, err = io.Copy(wr, r)

		if err != nil {
			return fmt.Errorf("Failed to copy %s to target, %w", rel_path, err)
		}

		err = wr.Close()

		if err != nil {
			return fmt.Errorf("Failed to close writer for %s, %w", rel_path, err)
		}

		atomic.AddUint64(&copied, 1)

		aa_log.Debug(opts.Logger, "Copied %s to %s", path, rel_path)
		return nil
	}

	iter, err := iterator.NewIterator(ctx, opts.IteratorURI, iter_cb)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new iterator, %w", err)
	}

	err = iter.IterateURIs(ctx, opts.IteratorSources...)

	if err != nil {
		return nil, fmt.Errorf("Failed to iterate URIs, %w", err)
	}

	stats := &CopyStats{
		Seen:    atomic.LoadUint64(&seen),
		Skipped: atomic.LoadUint64(&skipped),
		Copied:  atomic.LoadUint64(&copied),
	}

	return stats, nil
}
