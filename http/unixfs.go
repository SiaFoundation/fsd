package http

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	chunker "github.com/ipfs/boxo/chunker"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func (as *apiServer) handleUnixFSUpload(jc jape.Context) {
	defer jc.Request.Body.Close()

	opts := parseUnixFSOptions(jc)
	if opts == nil {
		return // error already handled
	}

	h := sha256.New()
	br := bufio.NewReaderSize(io.TeeReader(jc.Request.Body, h), 256<<20) // 256 MiB
	root, err := as.ipfs.UploadUnixFile(jc.Request.Context(), br, opts...)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	as.log.Debug("uploaded UnixFS file", zap.Stringer("cid", root.Cid()), zap.String("checksum", hex.EncodeToString(h.Sum(nil))))
	// return the calculated cid
	jc.Encode(root.Cid().String())
}

func parseUnixFSOptions(jc jape.Context) (opts []ipfs.UnixFSOption) {
	var cidVersion int
	if err := jc.DecodeForm("version", &cidVersion); err != nil {
		return nil
	}

	var builder cid.Builder
	switch cidVersion {
	case 0:
		builder = cid.V0Builder{}
	case 1:
		builder = cid.V1Builder{Codec: uint64(multicodec.DagPb), MhType: multihash.SHA2_256}
	default:
		jc.Error(fmt.Errorf("unsupported CID version: %d", cidVersion), http.StatusBadRequest)
		return nil
	}
	opts = append(opts, ipfs.UnixFSWithCIDBuilder(builder))

	var rawLeaves bool
	if err := jc.DecodeForm("rawLeaves", &rawLeaves); err != nil {
		return nil
	}
	opts = append(opts, ipfs.UnixFSWithRawLeaves(rawLeaves))

	maxlinks := ihelpers.DefaultLinksPerBlock
	if err := jc.DecodeForm("maxLinks", &maxlinks); err != nil {
		return nil
	}
	opts = append(opts, ipfs.UnixFSWithMaxLinks(maxlinks))

	blocksize := chunker.DefaultBlockSize
	if err := jc.DecodeForm("blockSize", &blocksize); err != nil {
		return nil
	}
	opts = append(opts, ipfs.UnixFSWithBlockSize(blocksize))

	return opts
}
