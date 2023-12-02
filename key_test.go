package main

import (
	"testing"

	"github.com/google/uuid"
)

func TestKeyBinaryMarshallingAndUnmarshalling(t *testing.T) {
	id := uuid.MustParse("a4a39129-7fb5-4855-9f80-6d290d52b812")

	keyItem := key{
		id:     &id,
		offset: 1024,
	}

	b, err := keyItem.MarshalBinary()
	if err != nil {
		t.Fatalf("binary marshalling failed: %v", err)
	}

	newKeyItem := key{id: &uuid.NullUUID{}}
	if err := newKeyItem.UnmarshalBinary(b); err != nil {
		t.Fatalf("binary unmarshalling failed: %v", err)
	}

	id_b, err := newKeyItem.id.MarshalBinary()
	if err != nil {
		t.Fatalf("new key binary marshalling failed: %v", err)
	}

	got_id, err := uuid.FromBytes(id_b)
	if err != nil {
		t.Fatalf("new key uuid parsing failed: %v", err)
	}

	if got_id.String() != "a4a39129-7fb5-4855-9f80-6d290d52b812" {
		t.Fatalf(`expected key id to be "%v"; got %s`, "a4a39129-7fb5-4855-9f80-6d290d52b812", got_id)
	}

	if newKeyItem.offset != 1024 {
		t.Fatalf("expected key offset to be %d; got %d", 1024, newKeyItem.offset)
	}
}

func TestKeyUnmarshallBinaryInvalidByteSliceSize(t *testing.T) {
	keyItem := key{id: &uuid.NullUUID{}}

	b := make([]byte, 8)
	err := keyItem.UnmarshalBinary(b)
	if err == nil {
		t.Fatal("expected error to exist; got nil")
	}
	if err.Error() != "invalid slice size" {
		t.Fatalf(`expected error to be "invalid slice size"; got "%s"`, err.Error())
	}

	b = make([]byte, 9)
	err = keyItem.UnmarshalBinary(b)
	if err == nil {
		t.Fatal("expected error to exist; got nil")
	}
	if err.Error() != "invalid UUID (got 1 bytes)" {
		t.Fatalf(`expected error to be "invalid UUID (got 1 bytes)"; got "%s"`, err.Error())
	}
}
