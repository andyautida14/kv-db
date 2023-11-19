package main

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const KeyIdSize = 16
const KeyOffsetSize = 8
const KeySize = KeyIdSize + KeyOffsetSize

type Key struct {
	Id     uuid.UUID
	Offset uint64
}

type KeyStorage struct {
	f *os.File
}

type KeyIndexer struct {
	s *KeyStorage
}

func (k *Key) MarshalBinary() ([]byte, error) {
	var b [KeySize]byte

	id, err := k.Id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	copy(b[:KeyIdSize], id[:])
	binary.LittleEndian.PutUint64(b[KeyIdSize:], k.Offset)
	return b[:], nil
}

func (k *Key) UnmarshalBinary(b []byte) error {
	if len(b) != KeySize {
		return errors.New("invalid slice size")
	}

	id, err := uuid.FromBytes(b[:KeyIdSize])
	if err != nil {
		return err
	}

	k.Id = id
	k.Offset = binary.LittleEndian.Uint64(b[KeyIdSize:])
	return nil
}

func (s *KeyStorage) ReadOffset(b []byte, off int64) (int, error) {
	if len(b) > KeySize {
		return 0, errors.New("slice length exceeded key size")
	}

	n, err := s.f.ReadAt(b, off*KeySize)
	if err != nil {
		return 0, errors.Wrap(err, "read from key storage by offset failed")
	}

	return n, nil
}

func (s *KeyStorage) WriteOffset(b []byte, off int64) (int, error) {
	if len(b) > KeySize {
		return 0, errors.New("slice length exceeded key size")
	}

	n, err := s.f.WriteAt(b, off*KeySize)
	if err != nil {
		return 0, errors.Wrap(err, "write to key storage by offset failed")
	}

	return n, nil
}

func (s *KeyStorage) Count() (int64, error) {
	stat, err := s.f.Stat()
	if err != nil {
		return -1, errors.Wrap(err, "counting key storage size failed")
	}
	return stat.Size() / KeySize, nil
}

func (s *KeyStorage) Reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

func (s *KeyIndexer) searchOffset(k []byte) (int64, bool, error) {
	count, err := s.s.Count()
	if err != nil {
		return -1, false, err
	}

	low := int64(0)
	high := count - 1
	median := int64(0)
	var medianKey [KeySize]byte

	for low <= high {
		median = (low + high) / 2

		if _, err := s.s.ReadOffset(medianKey[:], median); err != nil {
			return 0, false, err
		}

		switch bytes.Compare(medianKey[:], k) {
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

func (s *KeyIndexer) shiftRight(targetOffset int64) error {
	currentOffset, err := s.s.Count()
	if err != nil {
		return err
	}

	var b [KeySize]byte
	for currentOffset > targetOffset {
		if _, err := s.s.ReadOffset(b[:], currentOffset-1); err != nil {
			return err
		}

		if _, err := s.s.WriteOffset(b[:], currentOffset); err != nil {
			return err
		}

		currentOffset -= 1
	}

	return nil
}

func (s *KeyIndexer) Insert(key *Key) (int64, error) {
	id, err := key.Id.MarshalBinary()
	if err != nil {
		return -1, err
	}

	off, found, err := s.searchOffset(id)
	if err != nil {
		return -1, err
	}

	if !found {
		if err := s.shiftRight(off); err != nil {
			return -1, err
		}
	}

	k, err := key.MarshalBinary()
	if err != nil {
		return -1, err
	}

	if _, err := s.s.WriteOffset(k, off); err != nil {
		return -1, err
	}

	return off, nil
}

func (s *KeyIndexer) Find(key encoding.BinaryMarshaler) (*Key, error) {
	k, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	off, found, err := s.searchOffset(k)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	var b [KeySize]byte
	if _, err := s.s.ReadOffset(b[:], off); err != nil {
		return nil, err
	}

	foundKey := &Key{}
	if err := foundKey.UnmarshalBinary(b[:]); err != nil {
		return nil, err
	}

	return foundKey, nil
}

func NewKeyStorage(fileName string) (*KeyStorage, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &KeyStorage{f: f}, nil
}

func NewKeyIndexer(s *KeyStorage) *KeyIndexer {
	return &KeyIndexer{s: s}
}
