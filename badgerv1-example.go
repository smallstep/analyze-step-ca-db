package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/pkg/errors"
)

func main() {
	bo := badger.DefaultOptions(os.Args[1])
	bo.ValueLogLoadingMode = options.FileIO
	bo.Truncate = true

	db, err := badger.Open(bo)
	if err != nil {
		panic(errors.Wrap(err, "error opening Badger database"))
	}

	defer db.Close()

	for _, table := range tables {
		c := countPrefix(db, []byte(table))
		fmt.Printf("%s = %d\n", table, c)
	}
}

// badgerEncode encodes a byte slice into a section of a BadgerKey.
// See documentation for toBadgerKey.
func badgerEncode(val []byte) ([]byte, error) {
	l := len(val)
	switch {
	case l == 0:
		return nil, errors.Errorf("input cannot be empty")
	case l > 65535:
		return nil, errors.Errorf("length of input cannot be greater than 65535")
	default:
		lb := new(bytes.Buffer)
		if err := binary.Write(lb, binary.LittleEndian, uint16(l)); err != nil {
			return nil, errors.Wrap(err, "error doing binary Write")
		}
		return append(lb.Bytes(), val...), nil
	}
}

var tables = []string{
	"used_ott",
	"revoked_x509_certs",
	"x509_certs",
	"acme-accounts",
	"acme-keyID-accountID-index",
	"acme-authzs",
	"acme-challenges",
	"nonce-table",
	"acme-orders",
	"acme-account-orders-index",
	"acme-certs",
}

func countPrefix(db *badger.DB, prefix []byte) int {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions

	prefix, err := badgerEncode(prefix)
	if err != nil {
		panic(err)
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()
	count := 0
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		count++
	}
	return count
}
