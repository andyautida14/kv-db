package main

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

// const KeyIdSize = 16
const KeyOffsetSize = 8

// const KeySize = KeyIdSize + KeyOffsetSize

// type Key struct {
// 	Id     encoding.BinaryMarshaler
// 	Offset uint64
// }

type key struct {
	id     KeyId
	offset uint64
}

// func (k *Key) MarshalBinary() ([]byte, error) {
// 	var b [KeySize]byte

// 	id, err := k.Id.MarshalBinary()
// 	if err != nil {
// 		return nil, err
// 	}

// 	copy(b[:KeyIdSize], id[:])
// 	binary.LittleEndian.PutUint64(b[KeyIdSize:], k.Offset)
// 	return b[:], nil
// }

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

// func (k *Key) UnmarshalBinary(b []byte) error {
// 	if len(b) != KeySize {
// 		return errors.New("invalid slice size")
// 	}

// 	id, err := uuid.FromBytes(b[:KeyIdSize])
// 	if err != nil {
// 		return err
// 	}

// 	k.Id = id
// 	k.Offset = binary.LittleEndian.Uint64(b[KeyIdSize:])
// 	return nil
// }

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
