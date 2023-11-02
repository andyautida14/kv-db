package main

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const BookTitleSize = 4 * 32
const BookPageCountSize = 4
const BookSize = BookTitleSize + BookPageCountSize

type Book struct {
	Title     string
	PageCount uint32
}

type BookBuffer struct {
	buf [BookSize]byte
}

type BookReadWriter struct {
	f   *os.File
	off int64
}

func NewBookBufferFromBook(b *Book) *BookBuffer {
	bb := &BookBuffer{}
	bb.SetTitle(b.Title)
	bb.SetPageCount(b.PageCount)
	return bb
}

func NewBookBufferFromReader(r io.Reader) (*BookBuffer, error) {
	bb := &BookBuffer{}
	if _, err := bb.ReadFrom(r); err != nil {
		return nil, err
	}
	return bb, nil
}

func NewBookReadWriter(f *os.File, off int64) *BookReadWriter {
	return &BookReadWriter{f: f, off: off}
}

func (b *BookBuffer) Reset() {
	clear(b.buf[:])
}

func (b *BookBuffer) Title() string {
	return string(b.buf[:BookTitleSize-1])
}

func (b *BookBuffer) SetTitle(v string) {
	title_b := b.buf[:BookTitleSize-1]
	clear(title_b)
	copy(title_b, []byte(v+"\000"))
}

func (b *BookBuffer) PageCount() uint32 {
	return binary.LittleEndian.Uint32(b.buf[BookTitleSize:])
}

func (b *BookBuffer) SetPageCount(v uint32) {
	binary.LittleEndian.PutUint32(b.buf[BookTitleSize:], v)
}

func (b *BookBuffer) ToBook() *Book {
	return &Book{
		Title:     b.Title(),
		PageCount: b.PageCount(),
	}
}

func (b *BookBuffer) SetFromBook(book *Book) {
	b.SetTitle(book.Title)
	b.SetPageCount(book.PageCount)
}

func (b *BookBuffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(b.buf[:])
	return int64(n), err
}

func (b *BookBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.buf[:])
	return int64(n), err
}

func (b *BookReadWriter) Offset() int64 {
	return b.off
}

func (b *BookReadWriter) SetOffset(v int64) {
	b.off = v
}

func (b *BookReadWriter) Read(p []byte) (int, error) {
	if len(p) != BookSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.ReadAt(p, b.off*BookSize)
}

func (b *BookReadWriter) Write(p []byte) (int, error) {
	if len(p) != BookSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.WriteAt(p, b.off*BookSize)
}

func (b *BookReadWriter) Count() (int64, error) {
	stat, err := b.f.Stat()
	if err != nil {
		return 0, nil
	}

	return stat.Size() / BookSize, nil
}

func (b *BookReadWriter) Title() (string, error) {
	var title_buf [BookTitleSize]byte
	_, err := b.f.ReadAt(title_buf[:], b.off*BookSize)
	return string(title_buf[:]), err
}

func (b *BookReadWriter) SetTitle(v string) error {
	var title_buf [BookTitleSize]byte
	copy(title_buf[:], []byte(v+"\000"))
	_, err := b.f.WriteAt(title_buf[:], b.off*BookSize)
	return err
}

func (b *BookReadWriter) PageCount() (uint32, error) {
	var pageCount_buf [BookPageCountSize]byte
	_, err := b.f.ReadAt(pageCount_buf[:], (b.off*BookSize)+BookTitleSize)
	return binary.LittleEndian.Uint32(pageCount_buf[:]), err
}

func (b *BookReadWriter) SetPageCount(v uint32) error {
	var pageCount_buf [BookPageCountSize]byte
	binary.LittleEndian.PutUint32(pageCount_buf[:], v)
	_, err := b.f.WriteAt(pageCount_buf[:], (b.off*BookSize)+BookTitleSize)
	return err
}
