[![ipfsd](https://sia.tech/assets/banners/sia-banner-fsd.png)](http://sia.tech)

# fsd
[![GoDoc](https://godoc.org/go.sia.tech/fsd?status.svg)](https://godoc.org/go.sia.tech/fsd)
<a href="https://ipfs.tech"><img src="https://img.shields.io/badge/IPFS-Compatible-blue.svg" alt="IPFS Implementation"></a>

> **fsd** _(interplanetary **f**ile **s**ystem **d**aemon)_ is an IPFS daemon that stores blocks on Sia via `renterd`.

## usage

```sh
# upload a file
curl -X POST -H 'Content-Type: application/octet-stream' --data-binary @ape0.png http://localhost:8001/api/unixfs/upload
"QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ"
curl http://localhost:8080/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
```

## todo
- [ ] IPNS support
