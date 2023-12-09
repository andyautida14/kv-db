package main

import (
	"bytes"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/google/uuid"
)

func setupIndexerTest(tb testing.TB) (func(tb testing.TB), Storage, Indexer, []uuid.UUID) {
	if err := os.MkdirAll("./data/test", os.ModePerm); err != nil {
		tb.Fatalf("storage data directory creation failed: %v", err)
	}

	s, err := NewStorage("./data/test/indexer", KeySize)
	if err != nil {
		tb.Fatalf("storage creation failed: %v", err)
	}

	if err := s.Reset(); err != nil {
		tb.Fatalf("storage reset failed: %v", err)
	}

	indexer := NewIndexer(KeyIdSize)

	ids := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}

	for _, id := range ids {
		keyItem := &key{id: &id, offset: 0}
		if _, err := indexer.Insert(s, keyItem); err != nil {
			tb.Fatalf("indexer insertion failed: %v", err)
		}
	}

	return func(tb testing.TB) {
		s.Close()
	}, s, indexer, ids
}

func makeSortedIds(t *testing.T, ids []uuid.UUID) []uuid.UUID {
	sortedIds := make([]uuid.UUID, len(ids))
	copy(sortedIds, ids)

	sort.Slice(sortedIds, func(i, j int) bool {
		a, err := sortedIds[i].MarshalBinary()
		if err != nil {
			t.Fatalf("id binary marshalling failed: %v", err)
		}

		b, err := sortedIds[j].MarshalBinary()
		if err != nil {
			t.Fatalf("id binary marshalling failed: %v", err)
		}

		return bytes.Compare(a, b) == -1
	})

	return sortedIds
}

func TestIndexerInsert(t *testing.T) {
	teardown, s, _, ids := setupIndexerTest(t)
	defer teardown(t)

	sortedIds := makeSortedIds(t, ids)

	for offset, expectedId := range sortedIds {
		readKey := &key{id: &uuid.NullUUID{}}
		var b [KeySize]byte

		n, err := s.ReadOffset(b[:], int64(offset))
		if err != nil {
			t.Fatalf("storage read offset failed: %v", err)
		}

		if n != KeySize {
			t.Fatalf("expected number of bytes read from storage to be %d; got %d", KeySize, n)
		}

		if err := readKey.UnmarshalBinary(b[:]); err != nil {
			t.Fatalf("key item unmarshalling failed: %v", err)
		}

		readIdBytes, err := readKey.id.MarshalBinary()
		if err != nil {
			t.Fatalf("key id marshalling failed: %v", err)
		}

		actualId, err := uuid.FromBytes(readIdBytes)
		if err != nil {
			t.Fatalf("uuid parsing from bytes failed: %v", err)
		}

		if actualId != expectedId {
			t.Fatalf("expected to get id %v; got %v", expectedId, actualId)
		}

		if readKey.offset != 0 {
			t.Fatalf("expected key item offset to be 0; got %d", readKey.offset)
		}
	}
}

func TestIndexerInsertAlreadyExists(t *testing.T) {
	teardown, s, indexer, ids := setupIndexerTest(t)
	defer teardown(t)

	i := rand.Intn(5)
	expectedId := ids[i]
	keyItem := &key{id: &expectedId, offset: 1337}

	off, err := indexer.Insert(s, keyItem)
	if err != nil {
		t.Fatalf("indexer re-insertion failed: %v", err)
	}

	count, err := s.Count()
	if err != nil {
		t.Fatalf("storage count failed: %v", err)
	}

	if count != 5 {
		t.Fatalf("expected stored item count to be 5; got %d", count)
	}

	readKey := &key{id: &uuid.NullUUID{}}
	var b [KeySize]byte

	n, err := s.ReadOffset(b[:], off)
	if err != nil {
		t.Fatalf("storage read offset failed: %v", err)
	}

	if n != KeySize {
		t.Fatalf("expected number of bytes read from storage to be %d; got %d", KeySize, n)
	}

	if err := readKey.UnmarshalBinary(b[:]); err != nil {
		t.Fatalf("key item unmarshalling failed: %v", err)
	}

	readIdBytes, err := readKey.id.MarshalBinary()
	if err != nil {
		t.Fatalf("key id marshalling failed: %v", err)
	}

	actualId, err := uuid.FromBytes(readIdBytes)
	if err != nil {
		t.Fatalf("uuid parsing from bytes failed: %v", err)
	}

	if actualId != expectedId {
		t.Fatalf("expected to get id %v; got %v", expectedId, actualId)
	}

	if readKey.offset != 1337 {
		t.Fatalf("expected key item offset to be 1337; got %d", readKey.offset)
	}
}

func TestIndexerFind(t *testing.T) {
	teardown, s, indexer, ids := setupIndexerTest(t)
	defer teardown(t)

	readKey := &key{id: &uuid.NullUUID{}}
	var b [KeySize]byte
	for _, id := range ids {
		off, err := indexer.Find(s, &id)
		if err != nil {
			t.Fatalf("indexer find failed: %v", err)
		}

		if _, err := s.ReadOffset(b[:], off); err != nil {
			t.Fatalf("storage read offset failed: %v", err)
		}

		if err := readKey.UnmarshalBinary(b[:]); err != nil {
			t.Fatalf("key binary unmarshalling failed: %v", err)
		}

		readIdBytes, err := readKey.id.MarshalBinary()
		if err != nil {
			t.Fatalf("key id marshalling failed: %v", err)
		}

		actualId, err := uuid.FromBytes(readIdBytes)
		if err != nil {
			t.Fatalf("uuid parsing from bytes failed: %v", err)
		}

		if actualId != id {
			t.Fatalf("expected to get id %v; got %v", id, actualId)
		}

		if readKey.offset != 0 {
			t.Fatalf("expected key item offset to be 0; got %d", readKey.offset)
		}
	}
}

func TestIndexerFindDoesNotExist(t *testing.T) {
	teardown, s, indexer, _ := setupIndexerTest(t)
	defer teardown(t)

	unsavedId := uuid.New()
	offset, err := indexer.Find(s, &unsavedId)
	if err != nil {
		t.Fatalf("indexer find failed: %v", err)
	}

	if offset != -1 {
		t.Fatalf("expected offset to be %d; got %d", -1, offset)
	}
}

func TestIndexerRemove(t *testing.T) {
	teardown, s, indexer, ids := setupIndexerTest(t)
	defer teardown(t)

	idxToRemove := rand.Intn(5)
	idToRemove := ids[idxToRemove]
	idsToRemain := []uuid.UUID{}
	for i, id := range ids {
		if i != idxToRemove {
			idsToRemain = append(idsToRemain, id)
		}
	}

	if err := indexer.Remove(s, &idToRemove); err != nil {
		t.Fatalf("indexer remove failed: %v", err)
	}

	count, err := s.Count()
	if err != nil {
		t.Fatalf("storage count failed: %v", err)
	}

	if count != 4 {
		t.Fatalf("expected count to be %d; got %d", 4, count)
	}

	foundOffset, err := indexer.Find(s, &idToRemove)
	if err != nil {
		t.Fatalf("indexer find failed: %v", err)
	}

	if foundOffset != -1 {
		t.Fatalf("expected removed id to not be found")
	}

	for _, id := range idsToRemain {
		foundOffset, err := indexer.Find(s, &id)
		if err != nil {
			t.Fatalf("indexer find failed: %v", err)
		}

		if foundOffset == -1 {
			t.Fatalf("expected remaining id to be found")
		}
	}
}
