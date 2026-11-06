// Package zenbun provides building blocks for an S3-backed search index.
package zenbun

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound indicates that the requested object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrConflict indicates that a conditional write did not match the current object.
	ErrConflict = errors.New("conditional write conflict")
	// ErrInvalidRange indicates that a byte range cannot be satisfied.
	ErrInvalidRange = errors.New("invalid byte range")
)

// ByteRange specifies a half-open interval [Start, End). A nil range reads the
// entire object. End must be greater than Start and no larger than object size.
type ByteRange struct {
	Start int64
	End   int64
}

// Object describes the data and version returned by a read.
type Object struct {
	Data []byte
	ETag string
}

// ObjectInfo describes an object returned by List.
type ObjectInfo struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

// PutOptions selects at most one precondition. IfNoneMatch requires that the
// object does not exist; IfMatch requires the exact ETag of an existing object.
type PutOptions struct {
	IfNoneMatch bool
	IfMatch     string
}

// Store is the object storage contract used by zenbun. Implementations must
// apply conditional Put atomically across writers. ETags are opaque, and must
// be passed back unchanged. List returns all matching objects sorted by key;
// callers must not assume that a listing is a snapshot concurrent with writes.
//
// Implementations must be safe for concurrent use. Callers test returned
// errors with errors.Is against the package's sentinel errors.
type Store interface {
	// Get reads the object at key, or the byte range r of it when r is not nil.
	// It returns an error matching ErrNotFound if the object is absent, and
	// ErrInvalidRange if r cannot be satisfied.
	Get(ctx context.Context, key string, r *ByteRange) (Object, error)

	// Put writes data to key and returns the new object's ETag. It returns an
	// error matching ErrConflict if the precondition in options does not hold.
	Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error)

	// Delete removes the object at key. Deleting an absent object is not an
	// error, though implementations may return one matching ErrNotFound.
	Delete(ctx context.Context, key string) error

	// List returns the objects whose keys start with prefix, sorted by key.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}
