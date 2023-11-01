package main

import "encoding/binary"

const BookSize = (4 * 32) + 4

type Book struct {
	Title     string
	PageCount uint32
}

type BookBuffer struct {
	buf [(4 * 32) + 4]byte
}

func (b *BookBuffer) Reset() {
	clear(b.buf[:])
}

func (b *BookBuffer) Title() string {
	return string(b.buf[:(4*32)-1])
}

func (b *BookBuffer) SetTitle(v string) {
	title_b := b.buf[:(4*32)-1]
	clear(title_b)
	copy(title_b, []byte(v+"\000"))
}

func (b *BookBuffer) PageCount() uint32 {
	return binary.LittleEndian.Uint32(b.buf[4*32:])
}

func (b *BookBuffer) SetPageCount(v uint32) {
	binary.LittleEndian.PutUint32(b.buf[4*32:], v)
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

func (b *BookBuffer) ToWriterAt(w writerAt, off int) (int, error) {
	off_b := int64(BookSize * off)
	return w.WriteAt(b.buf[:], off_b)
}

func (b *BookBuffer) FromReaderAt(r readerAt, off int) (int, error) {
	off_b := int64(BookSize * off)
	return r.ReadAt(b.buf[:], off_b)
}

func NewBookBufferFromBook(b *Book) *BookBuffer {
	bb := BookBuffer{}
	bb.SetTitle(b.Title)
	bb.SetPageCount(b.PageCount)
	return &bb
}

func NewBookBufferFromReaderAt(r readerAt, off int) (*BookBuffer, error) {
	off_b := int64(BookSize * off)
	bb := BookBuffer{}
	if _, err := r.ReadAt(bb.buf[:], off_b); err != nil {
		return nil, err
	}
	return &bb, nil
}

func TotalBookCount(s fileStat) int64 {
	return s.Size() / BookSize
}

type readerAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

type writerAt interface {
	WriteAt(p []byte, off int64) (n int, err error)
}

type fileStat interface {
	Size() int64
}
