package main

import (
	"os"
	"testing"
)

func setupStorageTest(tb testing.TB) (func(tb testing.TB), Storage, []Book) {
	if err := os.MkdirAll("./data/test", os.ModePerm); err != nil {
		tb.Fatalf("storage data directory creation failed: %v", err)
	}

	s, err := NewStorage("./data/test/data", BookSize)
	if err != nil {
		tb.Fatalf("storage creation failed: %v", err)
	}

	if err := s.Reset(); err != nil {
		tb.Fatalf("storage reset failed: %v", err)
	}

	books := []Book{
		{
			Title: "Game of Thrones",
			Year:  1996,
		},
		{
			Title: "Harry Potter",
			Year:  1997,
		},
		{
			Title: "Lord of the Rings",
			Year:  1954,
		},
	}

	for i, book := range books {
		b, err := book.MarshalBinary()
		if err != nil {
			tb.Fatalf("book binary marshalling failed: %v", err)
		}

		n, err := s.WriteOffset(b, int64(i))
		if err != nil {
			tb.Fatalf("storage write offset failed: %v", err)
		}

		if n != BookSize {
			tb.Fatalf("expected number of bytes written to storage to be %d; got %d", BookSize, n)
		}
	}

	return func(tb testing.TB) {
		s.Close()
	}, s, books
}

func TestStorageReadWriteOffset(t *testing.T) {
	teardown, s, books := setupStorageTest(t)
	defer teardown(t)

	for i, book := range books {
		readBook := &Book{}
		var b [BookSize]byte

		n, err := s.ReadOffset(b[:], int64(i))
		if err != nil {
			t.Fatalf("storage read offset failed: %v", err)
		}

		if n != BookSize {
			t.Fatalf("expected number of bytes read from storage to be %d; got %d", BookSize, n)
		}

		if err := readBook.UnmarshalBinary(b[:]); err != nil {
			t.Fatalf("book binary unmarshalling failed: %v", err)
		}

		if readBook.Title != book.Title {
			t.Fatalf(`expected read book title to be "%s"; got "%s"`, book.Title, readBook.Title)
		}

		if readBook.Year != book.Year {
			t.Fatalf("expected read book year to be %d; got %d", book.Year, readBook.Year)
		}
	}
}

func TestStorageCount(t *testing.T) {
	teardown, s, _ := setupStorageTest(t)
	defer teardown(t)

	count, err := s.Count()
	if err != nil {
		t.Fatalf("storage count failed: %v", err)
	}

	if count != 3 {
		t.Fatalf("expected count of items saved in storage to be 3; got %d", count)
	}
}

func TestStorageOverwrite(t *testing.T) {
	teardown, s, books := setupStorageTest(t)
	defer teardown(t)

	updatedBook := &Book{
		Title: books[0].Title + " (Updated)",
		Year:  books[0].Year,
	}

	b, err := updatedBook.MarshalBinary()
	if err != nil {
		t.Fatalf("book binary marshalling failed: %v", err)
	}

	n, err := s.WriteOffset(b, 0)
	if err != nil {
		t.Fatalf("storage overwrite offset failed: %v", err)
	}

	if n != BookSize {
		t.Fatalf("expected number of bytes overwritten to storage to be %d; got %d", BookSize, n)
	}

	var read_b [BookSize]byte
	n, err = s.ReadOffset(read_b[:], 0)
	if err != nil {
		t.Fatalf("storage read offset failed: %v", err)
	}

	if n != BookSize {
		t.Fatalf("expected number of bytes read from storage to be %d; got %d", BookSize, n)
	}

	readBook := &Book{}
	if err := readBook.UnmarshalBinary(read_b[:]); err != nil {
		t.Fatalf("book binary unmarshalling failed: %v", err)
	}

	if readBook.Title != updatedBook.Title {
		t.Fatalf(`expected read book title to be "%s"; got "%s"`, updatedBook.Title, readBook.Title)
	}

	if readBook.Year != updatedBook.Year {
		t.Fatalf("expected read book year to be %d; got %d", updatedBook.Year, readBook.Year)
	}
}

func TestStorageShiftLeft(t *testing.T) {
	teardown, s, books := setupStorageTest(t)
	defer teardown(t)

	if err := s.ShiftLeft(1); err != nil {
		t.Fatalf("storage shift left failed: %v", err)
	}

	count, err := s.Count()
	if err != nil {
		t.Fatalf("storage count failed: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected count of items saved in storage to be 2; got %d", count)
	}

	updatedBooks := []Book{
		books[0],
		books[2],
	}

	for i, book := range updatedBooks {
		readBook := &Book{}
		var b [BookSize]byte

		n, err := s.ReadOffset(b[:], int64(i))
		if err != nil {
			t.Fatalf("storage read offset failed: %v", err)
		}

		if n != BookSize {
			t.Fatalf("expected number of bytes read from storage to be %d; got %d", BookSize, n)
		}

		if err := readBook.UnmarshalBinary(b[:]); err != nil {
			t.Fatalf("book binary unmarshalling failed: %v", err)
		}

		if readBook.Title != book.Title {
			t.Fatalf(`expected read book title to be "%s"; got "%s"`, book.Title, readBook.Title)
		}

		if readBook.Year != book.Year {
			t.Fatalf("expected read book year to be %d; got %d", book.Year, readBook.Year)
		}
	}
}

func TestStorageShiftRight(t *testing.T) {
	teardown, s, books := setupStorageTest(t)
	defer teardown(t)

	if err := s.ShiftRight(1); err != nil {
		t.Fatalf("storage shift right failed: %v", err)
	}

	count, err := s.Count()
	if err != nil {
		t.Fatalf("storage count failed: %v", err)
	}

	if count != 4 {
		t.Fatalf("expected count of items saved in storage to be 4; got %d", count)
	}

	updatedBooks := []Book{
		books[0],
		books[1],
		books[1],
		books[2],
	}

	for i, book := range updatedBooks {
		readBook := &Book{}
		var b [BookSize]byte

		n, err := s.ReadOffset(b[:], int64(i))
		if err != nil {
			t.Fatalf("storage read offset failed: %v", err)
		}

		if n != BookSize {
			t.Fatalf("expected number of bytes read from storage to be %d; got %d", BookSize, n)
		}

		if err := readBook.UnmarshalBinary(b[:]); err != nil {
			t.Fatalf("book binary unmarshalling failed: %v", err)
		}

		if readBook.Title != book.Title {
			t.Fatalf(`expected read book title to be "%s"; got "%s"`, book.Title, readBook.Title)
		}

		if readBook.Year != book.Year {
			t.Fatalf("expected read book year to be %d; got %d", book.Year, readBook.Year)
		}
	}
}

func TestStorageItemSize(t *testing.T) {
	teardown, s, _ := setupStorageTest(t)
	defer teardown(t)

	itemSize := s.ItemSize()

	if itemSize != BookSize {
		t.Fatalf("expected item size to be %d; got %d", BookSize, itemSize)
	}
}
