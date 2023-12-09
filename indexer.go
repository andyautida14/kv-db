package main

import (
	"bytes"

	"github.com/pkg/errors"
)

type indexer struct {
	keySize uint16
}

func (idx *indexer) binarySearch(s Storage, k []byte) (int64, bool, error) {
	count, err := s.Count()
	if err != nil {
		return -1, false, err
	}

	low := int64(0)
	high := count - 1
	median := int64(0)
	medianKey := make([]byte, idx.keySize)

	for low <= high {
		median = (low + high) / 2

		if _, err := s.ReadOffset(medianKey, median); err != nil {
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

func (idx *indexer) Insert(s Storage, item Item) (int64, error) {
	b, err := item.MarshalBinary()
	if err != nil {
		return -1, err
	}

	off, found, err := idx.binarySearch(s, b[:idx.keySize])
	if err != nil {
		return -1, err
	}

	if !found {
		if err := s.ShiftRight(off); err != nil {
			return -1, err
		}
	}

	if _, err := s.WriteOffset(b, off); err != nil {
		return -1, err
	}

	return off, nil
}

func (idx *indexer) Remove(s Storage, keyId KeyId) error {
	b, err := keyId.MarshalBinary()
	if err != nil {
		return err
	}

	if uint16(len(b)) != idx.keySize {
		return errors.New("invalid key id size")
	}

	off, found, err := idx.binarySearch(s, b)
	if err != nil {
		return err
	}

	if !found {
		return nil
	}

	return s.ShiftLeft(off)
}

func (idx *indexer) Find(s Storage, keyId KeyId) (int64, error) {
	b, err := keyId.MarshalBinary()
	if err != nil {
		return -1, nil
	}

	if uint16(len(b)) != idx.keySize {
		return -1, errors.New("invalid key id size")
	}

	off, found, err := idx.binarySearch(s, b)
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

func NewIndexer(keySize uint16) Indexer {
	return &indexer{keySize: keySize}
}
