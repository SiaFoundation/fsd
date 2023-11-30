package sia

import (
	"github.com/ipfs/go-cid"
)

type (
	// A RenterdData links IPFS block data to an object stored on a renterd node
	RenterdData struct {
		Bucket    string `json:"bucket"`
		Key       string `json:"key"`
		Offset    uint64 `json:"offset"`
		FileSize  uint64 `json:"filesize"`
		BlockSize uint64 `json:"blocksize"`
	}

	// RenterdMeta links IPFS block metadata to an object stored on a renterd
	// node
	RenterdMeta struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
	}

	// A Link is a link to another IPFS node
	Link struct {
		CID  cid.Cid `json:"cid"`
		Name string  `json:"name"`
		Size uint64  `json:"size"`
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
