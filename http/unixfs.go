package http

import (
	"bufio"
	"fmt"
	"net/http"

	chunker "github.com/ipfs/boxo/chunker"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/fsd/sia"
	"go.sia.tech/jape"
)

func (as *apiServer) handleUnixFSCalculate(jc jape.Context) {
	ctx := jc.Request.Context()

	body := jc.Request.Body
	defer body.Close()

	opts, ok := parseUnixFSOptions(jc)
	if !ok {
		return // error already handled
	}

	br := bufio.NewReaderSize(body, 256<<20) // 256 MiB
	blocks, err := as.sia.CalculateBlocks(ctx, br, opts)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(blocks)
}

func (as *apiServer) handleUnixFSUpload(jc jape.Context) {
	body := jc.Request.Body
	defer body.Close()

	opts, ok := parseUnixFSOptions(jc)
	if !ok {
		return // error already handled
	}

	br := bufio.NewReaderSize(body, 256<<20) // 256 MiB
	c, err := as.sia.UploadFile(jc.Request.Context(), br, opts)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// return the calculated cid
	jc.Encode(c.String())
}

func parseUnixFSOptions(jc jape.Context) (sia.UnixFSOptions, bool) {
	cidVersion := 1
	if err := jc.DecodeForm("version", &cidVersion); err != nil {
		return sia.UnixFSOptions{}, false
	}

	opts := sia.UnixFSOptions{
		MaxLinks:  ihelpers.DefaultLinksPerBlock,
		BlockSize: chunker.DefaultBlockSize,
	}
	switch cidVersion {
	case 0:
		opts.CIDBuilder = cid.V0Builder{}
	case 1:
		opts.CIDBuilder = cid.V1Builder{Codec: uint64(multicodec.DagPb), MhType: multihash.SHA2_256}
	default:
		jc.Error(fmt.Errorf("unsupported CID version: %d", cidVersion), http.StatusBadRequest)
		return sia.UnixFSOptions{}, false
	}

	if err := jc.DecodeForm("rawLeaves", &opts.RawLeaves); err != nil {
		return sia.UnixFSOptions{}, false
	} else if err := jc.DecodeForm("maxLinks", &opts.MaxLinks); err != nil {
		return sia.UnixFSOptions{}, false
	} else if err := jc.DecodeForm("blockSize", &opts.BlockSize); err != nil {
		return sia.UnixFSOptions{}, false
	}
	return opts, true
}
