package main

import (
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/fileblob"
	_ "github.com/aaronland/gocloud-blob-s3"
)

import (
	"context"
	"log"

	"github.com/whosonfirst/go-whosonfirst-blob/app/copy"
)

func main() {

	ctx := context.Background()
	logger := log.Default()

	err := copy.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to run copy application, %v", err)
	}
}
