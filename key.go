package main

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

const KeyOffsetSize = 8

type key struct {
	id     KeyId
	offset uint64
}

func (k *key) MarshalBinary() ([]byte, error) {
	id, err := k.id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	idSize := len(id)
	b := make([]byte, idSize+KeyOffsetSize)
	copy(b[:idSize], id[:])
	binary.LittleEndian.PutUint64(b[idSize:], k.offset)
	return b, nil
}

func (k *key) UnmarshalBinary(b []byte) error {
	idSize := len(b) - KeyOffsetSize
	if idSize <= 0 {
		return errors.New("invalid slice size")
	}

	if err := k.id.UnmarshalBinary(b[:idSize]); err != nil {
		return err
	}

	k.offset = binary.LittleEndian.Uint64(b[idSize:])
	return nil
}
