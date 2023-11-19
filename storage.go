package main

import (
	"os"

	"github.com/pkg/errors"
)

type Storage struct {
	f        *os.File
	ItemSize uint16
}

func (s *Storage) ReadOffset(b []byte, off int64) (int, error) {
	if uint16(len(b)) > s.ItemSize {
		return 0, errors.New("slice length exceeded item size")
	}

	n, err := s.f.ReadAt(b, off*int64(s.ItemSize))
	if err != nil {
		return 0, errors.Wrap(err, "read from storage by offset failed")
	}

	return n, nil
}

func (s *Storage) WriteOffset(b []byte, off int64) (int, error) {
	if uint16(len(b)) > s.ItemSize {
		return 0, errors.New("slice length exceeded item size")
	}

	n, err := s.f.WriteAt(b, off*int64(s.ItemSize))
	if err != nil {
		return 0, errors.Wrap(err, "write to storage by offset failed")
	}

	return n, nil
}

func (s *Storage) Count() (int64, error) {
	stat, err := s.f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "counting items in storage failed")
	}

	return stat.Size() / int64(s.ItemSize), nil
}

func (s *Storage) Reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

func NewStorage(filename string, itemSize uint16) (*Storage, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Storage{f: f, ItemSize: itemSize}, nil
}
