[![ipfsd](https://sia.tech/assets/banners/sia-banner-ipfsd.png)](http://sia.tech)

# ipfsd

[![GoDoc](https://godoc.org/go.sia.tech/ipfsd?status.svg)](https://godoc.org/go.sia.tech/ipfsd)

IPFS gateway backed by `renterd` storage.

## usage

```sh
# Upload Bored Ape 0 image
curl -X POST -F "file=@ape0.png" http://localhost:8080/upload
CID: QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
http://localhost:8080/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ

# CID matches whats in the official Bored Ape contract
# Image matches same CID fetched from ipfs.io
https://ipfs.io/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
```
<image width="100px" src="https://ipfs.io/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ" />

## todo
- [ ] store concatenated IPFS blocks instead of raw file
- [ ] add a database mapping IPFS block hashes to offsets
- [ ] optionally push to an IPFS node for actual p2p sharing
- [ ] directory support, eg: ipfs://CID/images/cat.png, ipfs://QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/0
