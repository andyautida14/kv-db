package main

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

const BookTitleSize = 4 * 32
const BookYearSize = 2
const BookSize = BookTitleSize + BookYearSize

type Book struct {
	Title string
	Year  uint16
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
