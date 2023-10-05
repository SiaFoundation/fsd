# ipfsd

## usage

```
curl -X POST -F "file=@example.txt" http://localhost:8080/upload
CID: QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN

curl http://localhost:8080/ipfs/QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN
hello
```

## todo
- [ ] save to configurable renterd instance
- [ ] store concatenated IPFS blocks instead of raw file
- [ ] add a database mapping IPFS block hashes to offsets
- [ ] optionally push to an IPFS node for actual p2p sharing