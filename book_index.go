package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const BookIdSize = 16
const BookOffsetSize = 8
const BookIndexSize = BookIdSize + BookOffsetSize

type BookIndex struct {
	Id     uuid.UUID
	Offset uint64
}

type BookIndexReadWriter struct {
	f *os.File
}

func NewBookIndexReadWriter(f *os.File) *BookIndexReadWriter {
	return &BookIndexReadWriter{f: f}
}

func (b *BookIndexReadWriter) Read(p []byte, off int64) (int, error) {
	if len(p) != BookIndexSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.ReadAt(p, off*BookIndexSize)
}

func (b *BookIndexReadWriter) Write(p []byte, off int64) (int, error) {
	if len(p) != BookIndexSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.WriteAt(p, off*BookIndexSize)
}

func (b *BookIndexReadWriter) ReadId(p []byte, off int64) (int, error) {
	if len(p) != BookIdSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.ReadAt(p, off*BookIndexSize)
}

func (b *BookIndexReadWriter) ReadOffset(p []byte, off int64) (int, error) {
	if len(p) != BookOffsetSize {
		return 0, errors.New("invalid byte slice size")
	}

	return b.f.ReadAt(p, (off*BookIndexSize)+BookIdSize)
}

func (b *BookIndexReadWriter) GetIndexByOffset(off int64) (*BookIndex, error) {
	var buf [BookIndexSize]byte
	if _, err := b.Read(buf[:], off); err != nil {
		return nil, err
	}

	id, err := uuid.FromBytes(buf[:BookIdSize])
	if err != nil {
		return nil, err
	}

	return &BookIndex{
		Id:     id,
		Offset: binary.LittleEndian.Uint64(buf[BookIdSize:]),
	}, nil
}

func (b *BookIndexReadWriter) Count() (int64, error) {
	stat, err := b.f.Stat()
	if err != nil {
		return 0, nil
	}

	return stat.Size() / BookIndexSize, nil
}

func BinarySearchOffset(id uuid.UUID, index *BookIndexReadWriter) (int64, bool, error) {
	count, err := index.Count()
	if count == 0 {
		return 0, false, err
	}

	low := int64(0)
	high := count - 1

	idBytes, err := id.MarshalBinary()
	if err != nil {
		return 0, false, err
	}

	var medianBytes [BookIdSize]byte
	median := int64(0)
	for low <= high {
		median = (low + high) / 2

		if _, err := index.ReadId(medianBytes[:], median); err != nil {
			return 0, false, err
		}

		switch bytes.Compare(medianBytes[:], idBytes[:]) {
		case -1:
			low = median + 1
		case 1:
			high = median - 1
		default:
			return median, true, nil
		}
	}

	return low, false, nil
}

func ShiftRight(index *BookIndexReadWriter, targetOff int64) error {
	count, err := index.Count()
	if count == 0 {
		return err
	}

	curOff := count
	// fmt.Println("count:", count, "targetOff:", targetOff)
	var buf [BookIndexSize]byte
	for curOff > targetOff {
		// fmt.Println("curOff:", curOff)
		if _, err := index.Read(buf[:], curOff-1); err != nil {
			return errors.Wrap(err, "index item read failed")
		}

		if _, err := index.Write(buf[:], curOff); err != nil {
			return err
		}

		curOff -= 1
	}

	return nil
}

func Insert(item *BookIndex, index *BookIndexReadWriter) error {
	off, found, err := BinarySearchOffset(item.Id, index)
	if err != nil {
		return errors.Wrap(err, "binary search failed")
	}

	fmt.Println("off:", off, "found:", found)

	if !found {
		if err := ShiftRight(index, off); err != nil {
			return err
		}
	}

	var buf [BookIndexSize]byte
	idBytes, err := item.Id.MarshalBinary()
	if err != nil {
		return err
	}

	copy(buf[:BookIdSize], idBytes)
	binary.LittleEndian.PutUint64(buf[BookIdSize:], item.Offset)

	_, err = index.Write(buf[:], off)
	return err
}

func LinearInsert(item *BookIndex, index *BookIndexReadWriter) error {
	var buf [BookIndexSize]byte
	idBytes, err := item.Id.MarshalBinary()
	if err != nil {
		return err
	}

	count, err := index.Count()
	if err != nil {
		return err
	}

	copy(buf[:BookIdSize], idBytes)
	binary.LittleEndian.PutUint64(buf[BookIdSize:], item.Offset)

	_, err = index.Write(buf[:], count)
	return err
}

func BinarySearch(id uuid.UUID, index *BookIndexReadWriter) (*BookIndex, error) {
	off, found, err := BinarySearchOffset(id, index)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("id not found")
	}

	fmt.Println("offset:", off)
	return index.GetIndexByOffset(off)
}

func LinearSearch(id uuid.UUID, index *BookIndexReadWriter) (*BookIndex, error) {
	count, err := index.Count()
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < count; i++ {
		record, err := index.GetIndexByOffset(i)
		if err != nil {
			return nil, err
		}

		if record.Id == id {
			return record, nil
		}
	}

	return nil, errors.New("id not found")
}
