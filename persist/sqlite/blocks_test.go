package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.sia.tech/fsd/renterd"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestBlockLocation(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "fsd.sqlite3"), log.Named("sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var cids []cid.Cid
	for i := 0; i < 10000; i++ {
		mh, err := multihash.Encode(frand.Bytes(32), multihash.SHA2_256)
		if err != nil {
			t.Fatal(err)
		}

		c := cid.NewCidV0(mh)
		c = cid.NewCidV1(c.Type(), c.Hash())
		err = db.Pin(renterd.PinnedBlock{
			Cid:       c,
			Bucket:    "test",
			ObjectKey: c.String(),
		})
		if err != nil {
			t.Fatal(err)
		}
		cids = append(cids, c)
	}

	c := cids[frand.Intn(len(cids))]
	start := time.Now()
	bucket, key, err := db.BlockLocation(c)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	} else if bucket != "test" || key != c.String() {
		t.Fatalf("unexpected result: %v %v", bucket, key)
	}
	log.Debug("block location", zap.Stringer("cid", c), zap.String("bucket", bucket), zap.String("key", key), zap.Duration("elapsed", elapsed))
}

func BenchmarkBlockLocation(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "fsd.sqlite3"), log.Named("sqlite3"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var cids []cid.Cid
	for i := 0; i < 10000; i++ {
		mh, err := multihash.Encode(frand.Bytes(32), multihash.SHA2_256)
		if err != nil {
			b.Fatal(err)
		}

		c := cid.NewCidV0(mh)
		c = cid.NewCidV1(c.Type(), c.Hash())
		err = db.Pin(renterd.PinnedBlock{
			Cid:       c,
			Bucket:    "test",
			ObjectKey: c.String(),
		})
		if err != nil {
			b.Fatal(err)
		}
		cids = append(cids, c)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c := cids[i%len(cids)]
		bucket, key, err := db.BlockLocation(c)
		if err != nil {
			b.Fatal(err)
		} else if bucket != "test" || key != c.String() {
			b.Fatalf("unexpected result: %v %v", bucket, key)
		}
	}
}
