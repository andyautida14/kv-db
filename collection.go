package main

import (
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type collection struct {
	dataStorage Storage
	keyStorage  Storage
	indexer     Indexer
}

func (c *collection) Put(id KeyId, item Item) error {
	keyOffset, err := c.indexer.Find(id)
	if err != nil {
		return err
	}

	b, err := item.MarshalBinary()
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

		key := &key{id: id, offset: uint64(dataOffset)}
		if _, err := c.indexer.Insert(key); err != nil {
			return err
		}
	} else {
		k := make([]byte, int(c.indexer.KeySize()))
		if _, err := c.keyStorage.ReadOffset(k, keyOffset); err != nil {
			return err
		}

		key := &key{}
		if err := key.UnmarshalBinary(k); err != nil {
			return err
		}

		if _, err := c.dataStorage.WriteOffset(b, int64(key.offset)); err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) Get(id KeyId, item Item) error {
	keyOffset, err := c.indexer.Find(id)
	if err != nil {
		return err
	}

	if keyOffset < 0 {
		return errors.New("item not found")
	}

	k := make([]byte, c.keyStorage.ItemSize())
	if _, err := c.keyStorage.ReadOffset(k, keyOffset); err != nil {
		return err
	}

	key := &key{id: &uuid.NullUUID{}}
	if err := key.UnmarshalBinary(k); err != nil {
		return err
	}

	b := make([]byte, c.dataStorage.ItemSize())
	if _, err := c.dataStorage.ReadOffset(b, int64(key.offset)); err != nil {
		return err
	}

	return item.UnmarshalBinary(b)
}

func (c *collection) Remove(id KeyId) error {
	return c.indexer.Remove(id)
}

func (c *collection) Count() (int64, error) {
	return c.keyStorage.Count()
}

func (c *collection) Reset() error {
	if err := c.keyStorage.Reset(); err != nil {
		return err
	}

	return c.dataStorage.Reset()
}

func (c *collection) Close() error {
	err1 := c.dataStorage.Close()
	err2 := c.keyStorage.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func NewCollection(dataPath string, keySize uint16, keyIdSize uint16, itemSize uint16) (Collection, error) {
	collectionDir := filepath.Join(dataPath, "book")
	if err := os.MkdirAll(collectionDir, os.ModePerm); err != nil {
		return nil, err
	}

	dataFile := filepath.Join(collectionDir, "data")
	keyFile := filepath.Join(collectionDir, "key")

	dataStorage, err := NewStorage(dataFile, itemSize)
	if err != nil {
		return nil, err
	}

	keyStorage, err := NewStorage(keyFile, keySize)
	if err != nil {
		return nil, err
	}

	indexer, err := NewIndexer(keyStorage, keyIdSize)
	if err != nil {
		return nil, err
	}

	return &collection{
		dataStorage: dataStorage,
		keyStorage:  keyStorage,
		indexer:     indexer,
	}, nil
}
