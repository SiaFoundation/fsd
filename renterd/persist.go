package renterd

import "github.com/ipfs/go-cid"

type (
	PinnedBlock struct {
		Cid       cid.Cid   `json:"cid"`
		Bucket    string    `json:"bucket"`
		ObjectKey string    `json:"objectKey"`
		Links     []cid.Cid `json:"links"`
	}

	MetadataStore interface {
		Pin(PinnedBlock) error
		Unpin(c cid.Cid) error

		Pinned(offset, limit int) (roots []cid.Cid, err error)
		BlockLocation(c cid.Cid) (bucket, key string, err error)
	}
)
