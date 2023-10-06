package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siapfs/ipfs"
	"go.uber.org/zap"
)

type (
	Store interface {
		GetBlock(cid.Cid) (ipfs.Block, error)
		AddBlocks(blocks []ipfs.Block) error
	}

	server struct {
		worker *worker.Client
		creds  RenterdCredentials
		ipfs   iface.CoreAPI

		store Store
		log   *zap.Logger

		bucket string
	}

	RenterdCredentials struct {
		Address  string `json:"address"`
		Password string `json:"password"`
	}
)

func (s *server) handleCalculate(jc jape.Context) {
	body := jc.Request.Body
	defer body.Close()

	blocks, err := ipfs.BuildBalancedCID("test", body)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(blocks)
}

func (s *server) handleUpload(jc jape.Context) {
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}
	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	body := jc.Request.Body

	pr, pw := io.Pipe()
	r := io.TeeReader(body, pw)
	uploadErr := make(chan error, 1)

	go func() {
		defer pw.Close()
		defer body.Close()
		defer close(uploadErr)

		_, err = s.worker.UploadObject(jc.Request.Context(), r, cid.Hash().B58String(), api.UploadWithBucket(s.bucket))
		if err != nil {
			uploadErr <- err
		}
	}()

	blocks, err := ipfs.BuildBalancedCID(cidStr, pr)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	if err := <-uploadErr; err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	if err := s.store.AddBlocks(blocks); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	// the root cid is the first block
	rootCID := blocks[0].CID
	jc.Encode(rootCID.Hash().B58String())
	s.log.Info("uploaded cid", zap.String("rootCID", rootCID.Hash().B58String()), zap.Int("blocks", len(blocks)))
}

func (s *server) handleIPFS(jc jape.Context) {
	var cidStr string
	if err := jc.DecodeParam("cid", &cidStr); err != nil {
		return
	}

	cid, err := cid.Parse(cidStr)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	block, err := s.store.GetBlock(cid)
	if errors.Is(ipfs.ErrNotFound, err) {
		node, err := s.ipfs.Unixfs().Get(jc.Request.Context(), path.IpfsPath(cid))
		if err != nil {
			jc.Error(err, http.StatusInternalServerError)
			return
		}
		switch node := node.(type) {
		case files.File:
			jc.ResponseWriter.WriteHeader(http.StatusOK)
			if _, err := io.Copy(jc.ResponseWriter, node); err != nil {
				s.log.Error("failed to copy file", zap.Error(err))
				return
			}
		default:
			jc.Error(fmt.Errorf("unsupported %T", node), http.StatusBadRequest)
		}
		return
	}

	s.log.Info("downloading block from renterd", zap.String("cid", cid.Hash().B58String()), zap.String("key", block.Key), zap.Uint64("offset", block.Offset), zap.Uint64("length", block.Length))
	// note: download object from bucket is broken
	reader, err := downloadObject(jc.Request.Context(), s.creds.Address, s.creds.Password, s.bucket, block.Key, block.Offset, block.Length)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		s.log.Error("failed to download object", zap.Error(err))
		return
	}
	defer reader.Close()

	jc.ResponseWriter.WriteHeader(http.StatusOK)
	if _, err := io.Copy(jc.ResponseWriter, reader); err != nil {
		s.log.Error("failed to copy file", zap.Error(err))
		return
	}
}

func NewServer(bucket string, renterdCreds RenterdCredentials, node iface.CoreAPI, ds Store, log *zap.Logger) http.Handler {
	s := &server{
		worker: worker.NewClient(renterdCreds.Address, renterdCreds.Password),
		creds:  renterdCreds,
		store:  ds,
		ipfs:   node,
		log:    log,

		bucket: bucket,
	}
	return jape.Mux(map[string]jape.Handler{
		"POST /api/cid/calculate": s.handleCalculate,
		"POST /api/upload/:cid":   s.handleUpload,
		"GET /ipfs/:cid":          s.handleIPFS,
	})
}
