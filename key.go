package main

import (
	"encoding/binary"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const KeyIdSize = 16
const KeyOffsetSize = 8
const KeySize = KeyIdSize + KeyOffsetSize

type Key struct {
	Id     uuid.UUID
	Offset uint64
}

func (k *Key) MarshalBinary() ([]byte, error) {
	var b [KeySize]byte

	id, err := k.Id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	copy(b[:KeyIdSize], id[:])
	binary.LittleEndian.PutUint64(b[KeyIdSize:], k.Offset)
	return b[:], nil
}

func (k *Key) UnmarshalBinary(b []byte) error {
	if len(b) != KeySize {
		return errors.New("invalid slice size")
	}

	id, err := uuid.FromBytes(b[:KeyIdSize])
	if err != nil {
		return err
	}

	k.Id = id
	k.Offset = binary.LittleEndian.Uint64(b[KeyIdSize:])
	return nil
}
