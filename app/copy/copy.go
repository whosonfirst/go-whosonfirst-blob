package copy

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-whosonfirst-blob/copy"
)

func Run(ctx context.Context, logger *log.Logger) error {

	fs := DefaultFlagSet()
	return RunWithFlagSet(ctx, fs, logger)
}

func RunWithFlagSet(ctx context.Context, fs *flag.FlagSet, logger *log.Logger) error {

	flagset.Parse(fs)

	sources := fs.Args()
	
	opts := &copy.CopyOptions{
		BucketURI:       target_bucket_uri,
		IteratorURI:     iterator_uri,
		IteratorSources: sources,
		Logger:          logger,
		ACL:             acl,
		Verbose: verbose,
	}

	stats, err := copy.Copy(ctx, opts)

	if err != nil {
		return fmt.Errorf("Failed to copy, %w", err)
	}

	logger.Printf("Copy complete. Items processed: %d Items copied: %d Items skipped: %d\n", stats.Seen, stats.Copied, stats.Skipped)
	return nil
}
