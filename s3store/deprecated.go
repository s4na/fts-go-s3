package s3store

// S3Config is the former name of Config.
//
// Deprecated: Use Config.
type S3Config = Config

// S3Store is the former name of Store.
//
// Deprecated: Use Store.
type S3Store = Store

// S3Error is the former name of Error.
//
// Deprecated: Use Error.
type S3Error = Error

// NewS3Store is the former name of New.
//
// Deprecated: Use New.
func NewS3Store(config Config) (*Store, error) { return New(config) }
