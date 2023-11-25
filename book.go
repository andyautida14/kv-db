package main

import (
	"encoding"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const BookTitleSize = 4 * 32
const BookYearSize = 2
const BookSize = BookTitleSize + BookYearSize

type Book struct {
	Title string
	Year  uint16
}

type BookCollection struct {
	dataStorage Storage
	keyStorage  Storage
	indexer     Indexer
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

func (c *BookCollection) Put(id encoding.BinaryMarshaler, book *Book) error {
	keyOffset, err := c.indexer.Find(id)
	if err != nil {
		return err
	}

	b, err := book.MarshalBinary()
	if err != nil {
		return err
	}

	if keyOffset < 0 {
		dataOffset, err := c.dataStorage.Count()
		if err != nil {
			return err
		}

		if _, err := c.dataStorage.WriteOffset(b, dataOffset); err != nil {
			return err
		}

		key := &Key{Id: id, Offset: uint64(dataOffset)}
		if _, err := c.indexer.Insert(key); err != nil {
			return err
		}
	} else {
		k := make([]byte, int(c.indexer.KeySize()))
		if _, err := c.keyStorage.ReadOffset(k, keyOffset); err != nil {
			return err
		}

		key := &Key{}
		if err := key.UnmarshalBinary(k); err != nil {
			return err
		}

		if _, err := c.dataStorage.WriteOffset(b, int64(key.Offset)); err != nil {
			return err
		}
	}

	return nil
}

func (c *BookCollection) Get(id encoding.BinaryMarshaler) (*Book, error) {
	keyOffset, err := c.indexer.Find(id)
	if err != nil {
		return nil, err
	}

	if keyOffset < 0 {
		return nil, errors.New("item not found")
	}

	k := make([]byte, c.keyStorage.ItemSize())
	if _, err := c.keyStorage.ReadOffset(k, keyOffset); err != nil {
		return nil, err
	}

	key := &Key{}
	if err := key.UnmarshalBinary(k); err != nil {
		return nil, err
	}

	b := make([]byte, c.dataStorage.ItemSize())
	if _, err := c.dataStorage.ReadOffset(b, int64(key.Offset)); err != nil {
		return nil, err
	}

	book := &Book{}
	if err := book.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return book, nil
}

func (c *BookCollection) Reset() error {
	if err := c.dataStorage.Reset(); err != nil {
		return err
	}

	return c.keyStorage.Reset()
}

func NewBookCollection(dataPath string) (*BookCollection, error) {
	collectionDir := filepath.Join(dataPath, "book")
	if err := os.MkdirAll(collectionDir, os.ModePerm); err != nil {
		return nil, err
	}

	dataFile := filepath.Join(collectionDir, "data")
	keyFile := filepath.Join(collectionDir, "key")

	dataStorage, err := NewStorage(dataFile, BookSize)
	if err != nil {
		return nil, err
	}

	keyStorage, err := NewStorage(keyFile, KeySize)
	if err != nil {
		return nil, err
	}

	indexer, err := NewIndexer(keyStorage, KeyIdSize)
	if err != nil {
		return nil, err
	}

	return &BookCollection{
		dataStorage: dataStorage,
		keyStorage:  keyStorage,
		indexer:     indexer,
	}, nil
}
