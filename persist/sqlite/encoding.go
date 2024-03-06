package sqlite

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
)

func dbEncode(obj any) any {
	switch obj := obj.(type) {
	case cid.Cid:
		return obj.String()
	case uint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, obj)
		return b
	case time.Time:
		return obj.Unix()
	default:
		panic(fmt.Sprintf("dbEncode: unsupported type %T", obj))
	}
}

type decodable struct {
	v any
}

// Scan implements the sql.Scanner interface.
func (d *decodable) Scan(src any) error {
	if src == nil {
		return errors.New("cannot scan nil into decodable")
	}

	switch src := src.(type) {
	case string:
		if v, ok := d.v.(*cid.Cid); ok {
			c, err := cid.Parse(src)
			if err != nil {
				return err
			}
			*v = c
			return nil
		}
	case []byte:
		if v, ok := d.v.(*uint64); ok {
			*v = binary.BigEndian.Uint64(src)
			return nil
		}
	case int64:
		switch v := d.v.(type) {
		case *uint64:
			*v = uint64(src)
		case *time.Time:
			*v = time.Unix(src, 0).UTC()
		}
		return nil
	}
	return fmt.Errorf("cannot scan %T to %T", src, d.v)
}

func dbDecode(obj any) sql.Scanner {
	return &decodable{obj}
}
