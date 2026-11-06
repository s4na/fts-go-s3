package gcsstore

// GCSConfig is the former name of Config.
//
// Deprecated: Use Config.
type GCSConfig = Config

// GCSStore is the former name of Store.
//
// Deprecated: Use Store.
type GCSStore = Store

// GCSError is the former name of Error.
//
// Deprecated: Use Error.
type GCSError = Error

// NewGCSStore is the former name of New.
//
// Deprecated: Use New.
func NewGCSStore(config Config) (*Store, error) { return New(config) }
