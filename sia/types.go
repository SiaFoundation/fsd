package sia

import (
	"github.com/ipfs/go-cid"
)

type (
	// A RenterdData links IPFS block data to an object stored on a renterd node
	RenterdData struct {
		Key       string
		Offset    uint64
		FileSize  uint64
		BlockSize uint64
	}

	// RenterdMeta links IPFS block metadata to an object stored on a renterd
	// node
	RenterdMeta struct {
		Key    string
		Offset uint64
		Length uint64
	}

	// A Link is a link to another IPFS node
	Link struct {
		CID  cid.Cid
		Name string
		Size uint64
	}

	// A Block is an IPFS chunk with metadata for efficient storage and
	// retrieval from a renterd object
	Block struct {
		CID      cid.Cid     `json:"cid"`
		Data     RenterdData `json:"data"`
		Metadata RenterdMeta `json:"metadata"`
		Links    []Link      `json:"links"`
	}
)
