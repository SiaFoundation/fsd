package sia

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"
)

func TestUploader(t *testing.T) {
	c := cid.MustParse("QmRtaUc7FYQvijR3FHDtyu5M1PXfp6NCJmN4gG8jmEJgpj")
	log := zaptest.NewLogger(t)

	dataBuf, metaBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	uploader := NewUnixFileUploader(c.Hash().B58String(), dataBuf, metaBuf, log)

	f, err := os.Open("/Users/n8maninger/Downloads/QmRtaUc7FYQvijR3FHDtyu5M1PXfp6NCJmN4gG8jmEJgpj.png")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	spl := chunker.NewSizeSplitter(f, chunker.DefaultBlockSize)

	dbp := ihelpers.DagBuilderParams{
		Maxlinks:  ihelpers.DefaultLinksPerBlock,
		RawLeaves: true,
		CidBuilder: cid.V1Builder{
			Codec:  cid.DagProtobuf,
			MhType: multihash.SHA2_256,
		},
		Dagserv: uploader,
	}
	db, err := dbp.New(spl)
	if err != nil {
		t.Fatal(err)
	}

	rootNode, err := balanced.Layout(db)
	if err != nil {
		t.Fatal(err)
	}

	if !rootNode.Cid().Equals(c) {
		t.Fatalf("expected cid %s, got %s", c, rootNode.Cid())
	} else if metaBuf.Len() != 46 {
		t.Fatalf("expected metadata size 46, got %d", metaBuf.Len())
	} else if dataBuf.Len() != 904060 {
		t.Fatalf("expected data size 904060, got %d", dataBuf.Len())
	}

	blocks := uploader.Blocks()

	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks, got %d", len(blocks))
	}

	for _, b := range blocks {
		metaStart, metaEnd := b.Metadata.Offset, b.Metadata.Offset+b.Metadata.Length
		dataStart, dataEnd := b.Data.Offset, b.Data.Offset+b.Data.BlockSize

		t.Log(b.CID, metaStart, metaEnd, dataStart, dataEnd)

		mb := metaBuf.Bytes()[metaStart:metaEnd]
		db := dataBuf.Bytes()[dataStart:dataEnd]

		t.Log(hex.EncodeToString(mb))

		var meta pb.Data
		if err := proto.Unmarshal(mb, &meta); err != nil {
			t.Fatal(err)
		}

		if len(db) != 0 {
			meta.Data = db
		}

		buf, err := proto.Marshal(&meta)
		if err != nil {
			t.Fatal(err)
		}

		node := merkledag.NodeWithData(buf)
		for _, link := range b.Links {
			node.AddRawLink(link.Name, &format.Link{
				Name: link.Name,
				Size: link.Size,
				Cid:  link.CID,
			})
		}

		raw, err := node.EncodeProtobuf(true)
		if err != nil {
			t.Fatal(err)
		}

		if b.CID.Version() != 1 {
			t.Fatal("expected cid version 1")
		}

		if b.CID.Hash().B58String() == "QmRtaUc7FYQvijR3FHDtyu5M1PXfp6NCJmN4gG8jmEJgpj" {
			t.Log("RAW", raw, len(raw))
		}

		if actual := node.Cid(); !actual.Equals(b.CID) {
			t.Fatalf("expected cid %s, got %s", b.CID, actual)
		}
		t.Log("success", node.Cid())
	}

	h := sha256.New()

	h.Write(metaBuf.Bytes()[18:24])
	t.Log(hex.EncodeToString(h.Sum(nil)), len(metaBuf.Bytes()[18:24]))
}

func TestTest(t *testing.T) {
	c := cid.MustParse("QmRtaUc7FYQvijR3FHDtyu5M1PXfp6NCJmN4gG8jmEJgpj")
	v1Cid := cid.NewCidV1(c.Type(), c.Hash())

	t.Fatal(v1Cid.StringOfBase(multibase.Base32))
}
