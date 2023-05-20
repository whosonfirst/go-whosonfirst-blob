package copy

import (
	"flag"

	"github.com/sfomuseum/go-flags/flagset"
)

var target_bucket_uri string
var iterator_uri string
var acl string

var verbose bool

func DefaultFlagSet() *flag.FlagSet {

	fs := flagset.NewFlagSet("sync")

	fs.StringVar(&target_bucket_uri, "target-bucket-uri", "", "")
	fs.StringVar(&iterator_uri, "iterator-uri", "", "")
	fs.StringVar(&acl, "acl", "public-read", "")

	fs.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	return fs
}
