package main

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

const BookTitleSize = 4 * 32
const BookYearSize = 2
const BookSize = BookTitleSize + BookYearSize
const KeyIdSize = 16
const KeySize = KeyIdSize + KeyOffsetSize

type Book struct {
	Title string
	Year  uint16
}

func (b *Book) MarshalBinary() ([]byte, error) {
	var buf [BookSize]byte
	copy(buf[:BookTitleSize], []byte(b.Title))
	binary.LittleEndian.PutUint16(buf[BookTitleSize:], b.Year)
	return buf[:], nil
}

func (book *Book) UnmarshalBinary(b []byte) error {
	if len(b) != BookSize {
		return errors.New("invalid slice size")
	}

	title_b := b[:bytes.IndexByte(b[:BookTitleSize], 0)]
	book.Title = string(title_b)
	book.Year = binary.LittleEndian.Uint16(b[BookTitleSize:])
	return nil
}

func NewBookCollection(dataPath string) (Collection, error) {
	return NewCollection(dataPath, KeySize, KeyIdSize, BookSize)
}
