package renterd

import "github.com/ipfs/go-cid"

type (
	// A PinnedBlock is an IPFS block that is stored on a renterd node.
	PinnedBlock struct {
		Cid       cid.Cid   `json:"cid"`
		Bucket    string    `json:"bucket"`
		ObjectKey string    `json:"objectKey"`
		Links     []cid.Cid `json:"links"`
	}

	// A MetadataStore is a store for IPFS block metadata. It is used to
	// link blocks to their location on a renterd node.
	MetadataStore interface {
		Pin(PinnedBlock) error
		Unpin(c cid.Cid) error

		Pinned(offset, limit int) (roots []cid.Cid, err error)
		BlockLocation(c cid.Cid) (bucket, key string, err error)
	}
)
