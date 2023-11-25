package main

import (
	"bytes"
	"encoding"

	"github.com/pkg/errors"
)

type Indexer struct {
	s       *Storage
	KeySize uint16
}

func (idx *Indexer) binarySearch(k []byte) (int64, bool, error) {
	count, err := idx.s.Count()
	if err != nil {
		return -1, false, err
	}

	low := int64(0)
	high := count - 1
	median := int64(0)
	medianKey := make([]byte, idx.KeySize)

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

func (idx *Indexer) Insert(item encoding.BinaryMarshaler) (int64, error) {
	b, err := item.MarshalBinary()
	if err != nil {
		return -1, err
	}

	off, found, err := idx.binarySearch(b[:idx.KeySize])
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

func (idx *Indexer) Find(keyId encoding.BinaryMarshaler) (int64, error) {
	b, err := keyId.MarshalBinary()
	if err != nil {
		return -1, nil
	}

	if uint16(len(b)) != idx.KeySize {
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

func NewIndexer(s *Storage, keySize uint16) (*Indexer, error) {
	if keySize > s.ItemSize {
		return nil, errors.New("key size exceeded the item size")
	}

	return &Indexer{s: s, KeySize: keySize}, nil
}
