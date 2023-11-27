package main

import "encoding"

type Item interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type KeyId interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Storage interface {
	ReadOffset([]byte, int64) (int, error)
	WriteOffset([]byte, int64) (int, error)
	ShiftLeft(int64) error
	ShiftRight(int64) error
	Count() (int64, error)
	Reset() error
	ItemSize() uint16
}

type Indexer interface {
	Insert(Item) (int64, error)
	Find(KeyId) (int64, error)
	Remove(KeyId) error
	KeySize() uint16
}

type Collection interface {
	Put(KeyId, Item) error
	Get(KeyId, Item) error
	Remove(KeyId) error
	Count() (int64, error)
	Reset() error
}
