package main

import (
	"bytes"
	"encoding"

	"github.com/pkg/errors"
)

type indexer struct {
	s       Storage
	keySize uint16
}

func (idx *indexer) binarySearch(k []byte) (int64, bool, error) {
	count, err := idx.s.Count()
	if err != nil {
		return -1, false, err
	}

	low := int64(0)
	high := count - 1
	median := int64(0)
	medianKey := make([]byte, idx.keySize)

	for low <= high {
		median = (low + high) / 2

		if _, err := idx.s.ReadOffset(medianKey, median); err != nil {
			return 0, false, err
		}

		switch bytes.Compare(medianKey, k) {
		case -1:
			low = median + 1
		case 1:
			high = median - 1
		default:
			return median, true, nil
		}
	}

	return low, false, nil
}

func (idx *indexer) Insert(item Item) (int64, error) {
	b, err := item.MarshalBinary()
	if err != nil {
		return -1, err
	}

	off, found, err := idx.binarySearch(b[:idx.keySize])
	if err != nil {
		return -1, err
	}

	if !found {
		if err := idx.s.ShiftRight(off); err != nil {
			return -1, err
		}
	}

	if _, err := idx.s.WriteOffset(b, off); err != nil {
		return -1, err
	}

	return off, nil
}

func (idx *indexer) Find(keyId encoding.BinaryMarshaler) (int64, error) {
	b, err := keyId.MarshalBinary()
	if err != nil {
		return -1, nil
	}

	if uint16(len(b)) != idx.keySize {
		return -1, errors.New("invalid key id size")
	}

	off, found, err := idx.binarySearch(b)
	if err != nil {
		return -1, err
	}

	if !found {
		return -1, nil
	}

	return off, nil
}

func (idx *indexer) KeySize() uint16 {
	return idx.keySize
}

func NewIndexer(s Storage, keySize uint16) (Indexer, error) {
	if keySize > s.ItemSize() {
		return nil, errors.New("key size exceeded the item size")
	}

	return &indexer{s: s, keySize: keySize}, nil
}
