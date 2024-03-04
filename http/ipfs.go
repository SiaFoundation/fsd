package http

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.uber.org/zap"
)

type ipfsGatewayServer struct {
	ipfs *ipfs.Node
	log  *zap.Logger

	config config.Config
}

func getURLCID(r *http.Request) (c cid.Cid, path []string, redirect bool, _ error) {
	host := r.Host
	if str := r.Header.Get("X-Forwarded-Host"); str != "" {
		host = str
	}

	path = strings.Split(strings.TrimSpace(strings.Trim(r.URL.Path, "/")), "/")
	if len(path) != 0 && path[0] == "" { // ignore leading slash
		path = path[1:]
	}

	// try to parse the subdomain as a CID
	hostParts := strings.Split(host, ".")
	cidStr := hostParts[0]
	if cid, err := cid.Parse(cidStr); err == nil {
		return cid, path, false, nil
	}

	// check if the path contains a CID
	if len(path) >= 2 && (path[0] == "ipfs" || path[0] == "ipns") {
		cidStr, path = path[1], path[2:]

		if c, err := cid.Parse(cidStr); err == nil {
			return c, path, true, nil
		}
	}
	return cid.Undef, nil, false, errors.New("no cid found")
}

func redirectPathCID(w http.ResponseWriter, r *http.Request, c cid.Cid, path []string, log *zap.Logger) {
	host := r.Host
	if str := r.Header.Get("X-Forwarded-Host"); str != "" {
		host = str
	}
	scheme := "http://"
	if str := r.Header.Get("X-Forwarded-Proto"); str != "" {
		scheme = str + "://"
	}

	ustr := scheme + c.String() + ".ipfs." + host
	u, err := url.Parse(ustr)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		log.Error("failed to parse url", zap.Error(err), zap.String("url", ustr))
		return
	}
	u.RawPath = ""
	u.Path = "/" + strings.Join(path, "/")
	u.RawQuery = r.URL.RawQuery

	log.Debug("redirecting", zap.Stringer("cid", c), zap.String("url", u.String()), zap.String("path", u.Path))
	http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
}

func (is *ipfsGatewayServer) fetchAllowed(ctx context.Context, c cid.Cid) bool {
	// check if the file is locally pinned
	if has, err := is.ipfs.HasBlock(ctx, c); err != nil {
		is.log.Error("failed to check block existence", zap.Error(err))
	} else if has {
		return true
	}

	if is.config.IPFS.Gateway.Fetch.Enabled {
		if len(is.config.IPFS.Gateway.Fetch.AllowList) > 0 {
			for _, match := range is.config.IPFS.Gateway.Fetch.AllowList {
				if c.Equals(match) {
					return true
				}
			}
			return false
		}
		return true
	}
	return false
}

func buildDispositionHeader(params url.Values) string {
	disposition := "inline"
	if download, ok := params["download"]; ok && strings.EqualFold(download[0], "true") {
		disposition = "attachment"
	}
	if filename, ok := params["filename"]; ok {
		disposition += "; filename=" + filename[0]
	}
	return disposition

}

func (is *ipfsGatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	c, path, redirect, err := getURLCID(r)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	// redirect v0 to v1
	if c.Version() == 0 {
		c = cid.NewCidV1(c.Type(), c.Hash())
		redirect = true
	}

	if redirect && is.config.IPFS.Gateway.RedirectPathStyle {
		redirectPathCID(w, r, c, path, is.log.Named("redirect"))
		return
	}

	if !is.fetchAllowed(ctx, c) {
		http.Error(w, "", http.StatusNotFound)
		is.log.Info("remote fetch denied", zap.Stringer("cid", c))
		return
	}

	query, _ := url.ParseQuery(r.URL.RawQuery)

	w.Header().Set("Content-Disposition", buildDispositionHeader(query))

	var queryFormat string
	if len(query["format"]) > 0 {
		queryFormat = query["format"][0]
	}

	var format string
	switch {
	case strings.EqualFold(r.Header.Get("Accept"), "application/vnd.ipld.raw"), strings.EqualFold(queryFormat, "raw"):
		format = "raw"
	case strings.EqualFold(r.Header.Get("Accept"), "application/vnd.ipld.car"), strings.EqualFold(queryFormat, "car"):
		format = "car"
	}

	log := is.log.Named("serve").With(zap.Stringer("cid", c), zap.Strings("path", path))
	log.Info("serving content")

	switch format {
	case "car":
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusNotImplemented)
	case "raw":
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")

		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		block, err := is.ipfs.GetBlock(ctx, c)
		if err != nil {
			is.log.Error("failed to get block", zap.Error(err))
			http.Error(w, "unable to get block", http.StatusNotFound)
			return
		}
		w.Write(block.RawData())
	default:
		rsc, err := is.ipfs.DownloadUnixFile(ctx, c, path)
		if err != nil {
			http.Error(w, "", http.StatusNotFound)
			is.log.Error("failed to download cid", zap.Error(err))
			return
		}
		defer rsc.Close()
		http.ServeContent(w, r, "", time.Now(), rsc)
	}
}

// NewIPFSGatewayHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSGatewayHandler(ipfs *ipfs.Node, cfg config.Config, log *zap.Logger) http.Handler {
	return &ipfsGatewayServer{
		ipfs: ipfs,
		log:  log,

		config: cfg,
	}
}
