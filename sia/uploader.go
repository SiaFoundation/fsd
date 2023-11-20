package sia

import (
	"context"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.uber.org/zap"
)

// A UnixFileUploader uploads a UnixFS DAG to a renterd node
type UnixFileUploader struct {
	dataOffset uint64
	metaOffset uint64
	fileSize   uint64

	key string

	data     io.Writer
	metadata io.Writer

	blocks []Block

	log *zap.Logger
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (ufs *UnixFileUploader) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	panic("not implemented")
}

// GetMany returns a channel of NodeOptions given a set of CIDs.
func (ufs *UnixFileUploader) GetMany(ctx context.Context, c []cid.Cid) <-chan *format.NodeOption {
	panic("not implemented")
}

func (ufs *UnixFileUploader) uploadProtoNode(ctx context.Context, node *merkledag.ProtoNode) (data, metadata []byte, _ error) {
	fsNode, err := unixfs.ExtractFSNode(node)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract fs node: %w", err)
	}

	switch fsNode.Type() {
	case unixfs.TFile:
		data = fsNode.Data()

		buf, err := fsNode.GetBytes()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get bytes: %w", err)
		}

		var meta pb.Data
		if err := proto.Unmarshal(buf, &meta); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
		meta.Data = nil
		metadata, err = proto.Marshal(&meta)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}
	return
}

func (ufs *UnixFileUploader) uploadRawNode(ctx context.Context, node *merkledag.RawNode) (data, metadata []byte, _ error) {
	return node.RawData(), nil, nil
}

// Add adds a node to this DAG.
func (ufs *UnixFileUploader) Add(ctx context.Context, node format.Node) error {
	var data, metadata []byte
	var err error
	switch node := node.(type) {
	case *merkledag.ProtoNode:
		data, metadata, err = ufs.uploadProtoNode(ctx, node)
	case *merkledag.RawNode:
		data, metadata, err = ufs.uploadRawNode(ctx, node)
	default:
		return fmt.Errorf("unsupported node type: %T", node)
	}

	if err != nil {
		return err
	}

	dataSize := uint64(len(data))
	fileSize := ufs.fileSize + dataSize
	dataOffset := ufs.dataOffset + dataSize - fileSize

	ufs.log.Debug("adding node",
		zap.Stringer("cid", node.Cid()),
		zap.Uint64("filesize", fileSize),
		zap.Uint64("dataOffset", dataOffset),
		zap.Uint64("metaOffset", ufs.metaOffset),
		zap.Uint64("datasize", dataSize),
		zap.Int("links", len(node.Links())))

	metaLen, err := ufs.metadata.Write(metadata)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	_, err = ufs.data.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	var links []Link
	for _, link := range node.Links() {
		links = append(links, Link{
			CID:  link.Cid,
			Name: link.Name,
			Size: link.Size,
		})
	}

	block := Block{
		CID:   node.Cid(),
		Links: links,
		Data: RenterdData{
			Key:       ufs.key,
			Offset:    dataOffset,
			FileSize:  fileSize,
			BlockSize: dataSize,
		},
		Metadata: RenterdMeta{
			Key:    ufs.key + ".meta",
			Offset: ufs.metaOffset,
			Length: uint64(metaLen),
		},
	}
	ufs.dataOffset += dataSize
	ufs.fileSize += dataSize
	ufs.metaOffset += block.Metadata.Length
	ufs.blocks = append(ufs.blocks, block)
	return nil
}

// AddMany adds many nodes to this DAG.
//
// Consider using the Batch NodeAdder (`NewBatch`) if you make
// extensive use of this function.
func (ufs *UnixFileUploader) AddMany(ctx context.Context, nodes []format.Node) error {
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := ufs.Add(ctx, node); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes a node from this DAG.
//
// Remove returns no error if the requested node is not present in this DAG.
func (ufs *UnixFileUploader) Remove(context.Context, cid.Cid) error {
	panic("not implemented")
}

// RemoveMany removes many nodes from this DAG.
//
// It returns success even if the nodes were not present in the DAG.
func (ufs *UnixFileUploader) RemoveMany(context.Context, []cid.Cid) error {
	panic("not implemented")
}

// Blocks returns all blocks that were added to this DAG. They should be added
// to the blockstore
func (ufs *UnixFileUploader) Blocks() []Block {
	return ufs.blocks
}

// NewUnixFileUploader creates a new UnixFileUploader that uploads a UnixFS DAG
// to a renterd node.
func NewUnixFileUploader(key string, data, metadata io.Writer, log *zap.Logger) *UnixFileUploader {
	return &UnixFileUploader{
		log: log,

		key:      key,
		data:     data,
		metadata: metadata,
	}
}
