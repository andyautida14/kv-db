package main

import (
	"encoding/binary"
	"os"

	"github.com/pkg/errors"
)

const BookTitleSize = 4 * 32
const BookYearSize = 2
const BookSize = BookTitleSize + BookYearSize

type Book struct {
	Title string
	Year  uint16
}

type BookStorage struct {
	f *os.File
}

func (b *Book) MarshalBinary() ([]byte, error) {
	var buf [BookSize]byte
	copy(buf[:BookTitleSize-1], []byte(b.Title+"\000"))
	binary.LittleEndian.PutUint16(buf[BookTitleSize:], b.Year)
	return buf[:], nil
}

func (book *Book) UnmarshalBinary(b []byte) error {
	if len(b) != BookSize {
		return errors.New("invalid slice size")
	}

	book.Title = string(b[:BookTitleSize])
	book.Year = binary.LittleEndian.Uint16(b[BookTitleSize:])
	return nil
}

func (s *BookStorage) ReadOffset(b []byte, off int64) (int, error) {
	if len(b) > BookSize {
		return 0, errors.New("slice length exceeded entity size")
	}

	n, err := s.f.ReadAt(b, off*BookSize)
	if err != nil {
		return 0, errors.Wrap(err, "read from book storage by offset failed")
	}

	return n, nil
}

func (s *BookStorage) WriteOffset(b []byte, off int64) (int, error) {
	if len(b) > BookSize {
		return 0, errors.New("slice length exceeded entity size")
	}

	n, err := s.f.WriteAt(b, off*BookSize)
	if err != nil {
		return 0, errors.Wrap(err, "write to book storage by offset failed")
	}

	return n, nil
}

func (s *BookStorage) Count() (int64, error) {
	stat, err := s.f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "counting book storage size failed")
	}
	return stat.Size() / BookSize, nil
}

func (s *BookStorage) Reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

func NewBookStorage(fileName string) (*BookStorage, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &BookStorage{f: f}, nil
}
