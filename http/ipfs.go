package http

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ipfs/go-cid"
	"go.sia.tech/fsd/config"
	"go.sia.tech/fsd/ipfs"
	"go.sia.tech/fsd/sia"
	"go.uber.org/zap"
)

type ipfsGatewayServer struct {
	ipfs *ipfs.Node
	sia  *sia.Node
	log  *zap.Logger

	config config.Config
}

func getURLCID(r *http.Request) (c cid.Cid, path []string, redirect bool, _ error) {
	host := r.Host
	if str := r.Header.Get("X-Forwarded-Host"); str != "" {
		host = str
	}

	// try to parse the subdomain as a CID
	hostParts := strings.Split(host, ".")
	cidStr := hostParts[0]
	if cid, err := cid.Parse(cidStr); err == nil {
		return cid, strings.Split(r.URL.Path, "/")[1:], false, nil
	}

	path = strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// check if the path contains a CID
	if len(path) >= 2 && path[0] == "ipfs" || path[0] == "ipns" {
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
	log.Debug("path", zap.Strings("path", path))
	u.RawPath = ""
	u.Path = "/" + strings.Join(path, "/")
	u.RawQuery = r.URL.RawQuery

	log.Debug("redirecting", zap.Stringer("cid", c), zap.String("url", u.String()), zap.String("path", u.Path))
	http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
}

func (is *ipfsGatewayServer) allowRemoteFetch(c cid.Cid) bool {
	if !is.config.IPFS.Fetch.AllowRemote {
		return false // deny all
	} else if len(is.config.IPFS.Fetch.AllowList) == 0 {
		return true // allow all
	}

	// allowlist check
	for _, match := range is.config.IPFS.Fetch.AllowList {
		if c.Equals(match) {
			return true
		}
	}
	return false
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

	is.log.Info("serving", zap.Stringer("cid", c), zap.String("path", r.URL.Path))

	// TODO: support paths in Sia proxied downloads
	err = is.sia.ProxyHTTPDownload(c, r, w)
	if errors.Is(err, sia.ErrNotFound) && is.allowRemoteFetch(c) {
		is.log.Info("downloading from ipfs", zap.Stringer("cid", c))
		r, err := is.ipfs.DownloadCID(ctx, c, path)
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			is.log.Error("failed to download cid", zap.Error(err))
			return
		}
		defer r.Close()

		io.Copy(w, r)
	} else if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		is.log.Error("failed to get block", zap.Error(err))
	}
}

// NewIPFSGatewayHandler creates a new http.Handler for the IPFS gateway.
func NewIPFSGatewayHandler(ipfs *ipfs.Node, sia *sia.Node, cfg config.Config, log *zap.Logger) http.Handler {
	return &ipfsGatewayServer{
		ipfs: ipfs,
		sia:  sia,
		log:  log,

		config: cfg,
	}
}
