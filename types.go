package main

import "encoding"

type Item interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Storage interface {
	ReadOffset([]byte, int64) (int, error)
	WriteOffset([]byte, int64) (int, error)
	ShiftRight(int64) error
	Count() (int64, error)
	Reset() error
	ItemSize() uint16
}

type Indexer interface {
	Insert(Item) (int64, error)
	Find(encoding.BinaryMarshaler) (int64, error)
	KeySize() uint16
}
