package main

import (
	"testing"

	"github.com/google/uuid"
)

func setupCollectionTest(tb testing.TB) (func(tb testing.TB), Collection, []uuid.UUID, []Book) {
	c, err := NewCollection("./data/test", KeySize, KeyIdSize, BookSize)
	if err != nil {
		tb.Fatalf("collection creation failed: %v", err)
	}

	if err := c.Reset(); err != nil {
		tb.Fatalf("collection reset failed: %v", err)
	}

	ids := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
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
		{
			Title: "The Little Prince",
			Year:  1943,
		},
	}

	for i, id := range ids {
		if err := c.Put(&id, &books[i]); err != nil {
			tb.Fatalf("collection put failed: %v", err)
		}
	}

	return func(tb testing.TB) {
		c.Close()
	}, c, ids, books
}

func TestCollectionPutGet(t *testing.T) {
	teardown, c, ids, books := setupCollectionTest(t)
	defer teardown(t)

	book := &Book{}
	for i, id := range ids {
		if err := c.Get(&id, book); err != nil {
			t.Fatalf("collection get failed: %v", err)
		}

		expected := books[i]
		if book.Title != expected.Title {
			t.Fatalf(`expected book title to be "%v"; got "%v"`, expected.Title, book.Title)
		}

		if book.Year != expected.Year {
			t.Fatalf("expected book year to be %d; got %d", expected.Year, book.Year)
		}
	}
}

func TestCollectionCount(t *testing.T) {
	teardown, c, _, _ := setupCollectionTest(t)
	defer teardown(t)

	count, err := c.Count()
	if err != nil {
		t.Fatalf("collection count failed: %v", err)
	}

	if count != 4 {
		t.Fatalf("expected item count to be 4; got %d", count)
	}
}

func TestCollectionUpdate(t *testing.T) {
	teardown, c, ids, _ := setupCollectionTest(t)
	defer teardown(t)

	id := &ids[1]
	updated := &Book{
		Title: "Harry Potter and the Order of the Phoenix",
		Year:  2003,
	}

	if err := c.Put(id, updated); err != nil {
		t.Fatalf("collection update failed: %v", err)
	}

	count, err := c.Count()
	if err != nil {
		t.Fatalf("collection count failed: %v", err)
	}

	if count != 4 {
		t.Fatalf("expected count to still be 4 after update; got %v", count)
	}

	book := &Book{}
	if err := c.Get(id, book); err != nil {
		t.Fatalf("collection get updated failed: %v", err)
	}

	if book.Title != updated.Title {
		t.Fatalf(`expected updated book title to be "%v"; got "%v"`, updated.Title, book.Title)
	}

	if book.Year != updated.Year {
		t.Fatalf("expected updated book year to be %d; got %d", updated.Year, book.Year)
	}
}

func TestCollectionRemove(t *testing.T) {
	teardown, c, ids, books := setupCollectionTest(t)
	defer teardown(t)

	idToRemove := ids[2]
	remainingIds := []uuid.UUID{
		ids[0],
		ids[1],
		ids[3],
	}
	remainingBooks := []Book{
		books[0],
		books[1],
		books[3],
	}

	if err := c.Remove(&idToRemove); err != nil {
		t.Fatalf("collection remove failed: %v", err)
	}

	count, err := c.Count()
	if err != nil {
		t.Fatalf("collection count failed: %v", err)
	}

	if count != 3 {
		t.Fatalf("expected remaining items count to be 3; got %d", count)
	}

	book := &Book{}
	for i, id := range remainingIds {
		if err := c.Get(&id, book); err != nil {
			t.Fatalf("collection get failed: %v", err)
		}

		if book.Title != remainingBooks[i].Title {
			t.Fatalf(`expected book title to be "%v"; got "%v"`, remainingBooks[i].Title, book.Title)
		}

		if book.Year != remainingBooks[i].Year {
			t.Fatalf("expected book year to be %d; got %d", remainingBooks[i].Year, book.Year)
		}
	}
}
