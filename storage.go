package main

import (
	"os"

	"github.com/pkg/errors"
)

type storage struct {
	f        *os.File
	itemSize uint16
}

func (s *storage) ReadOffset(b []byte, off int64) (int, error) {
	if uint16(len(b)) > s.itemSize {
		return 0, errors.New("slice length exceeded item size")
	}

	n, err := s.f.ReadAt(b, off*int64(s.itemSize))
	if err != nil {
		return 0, errors.Wrap(err, "read from storage by offset failed")
	}

	return n, nil
}

func (s *storage) WriteOffset(b []byte, off int64) (int, error) {
	if uint16(len(b)) > s.itemSize {
		return 0, errors.New("slice length exceeded item size")
	}

	n, err := s.f.WriteAt(b, off*int64(s.itemSize))
	if err != nil {
		return 0, errors.Wrap(err, "write to storage by offset failed")
	}

	return n, nil
}

func (s *storage) ShiftLeft(targetOffset int64) error {
	count, err := s.Count()
	if err != nil {
		return err
	}

	if count == 0 {
		return nil
	}

	currentOffset := targetOffset
	lastOffset := count - 1
	b := make([]byte, s.itemSize)
	for currentOffset < lastOffset {
		if _, err := s.ReadOffset(b, currentOffset+1); err != nil {
			return err
		}

		if _, err := s.WriteOffset(b, currentOffset); err != nil {
			return err
		}

		currentOffset += 1
	}

	return s.f.Truncate((count - 1) * int64(s.itemSize))
}

func (s *storage) ShiftRight(targetOffset int64) error {
	currentOffset, err := s.Count()
	if err != nil {
		return err
	}

	b := make([]byte, s.itemSize)
	for currentOffset > targetOffset {
		if _, err := s.ReadOffset(b, currentOffset-1); err != nil {
			return err
		}

		if _, err := s.WriteOffset(b, currentOffset); err != nil {
			return err
		}

		currentOffset -= 1
	}

	return nil
}

func (s *storage) Count() (int64, error) {
	stat, err := s.f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "counting items in storage failed")
	}

	return stat.Size() / int64(s.itemSize), nil
}

func (s *storage) Reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

func (s *storage) ItemSize() uint16 {
	return s.itemSize
}

func NewStorage(filename string, itemSize uint16) (Storage, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &storage{f: f, itemSize: itemSize}, nil
}
