package main

import (
	"testing"
)

func TestBinaryMarshallingAndUnmarshalling(t *testing.T) {
	book := Book{
		Title: "Game of Thrones",
		Year:  1996,
	}

	b, err := book.MarshalBinary()
	if err != nil {
		t.Fatalf("binary marshalling failed: %v", err)
	}

	newBook := Book{}
	if err := newBook.UnmarshalBinary(b); err != nil {
		t.Fatalf("binary unmarshalling failed: %v", err)
	}

	if newBook.Title != "Game of Thrones" {
		t.Fatalf(`expected title to be "%s"; got "%s" %d`, "Game of Thrones", newBook.Title, len(newBook.Title))
	}

	if newBook.Year != 1996 {
		t.Fatalf("expected year to be %d; got %d", 1996, newBook.Year)
	}
}

func TestUnmarshallBinaryInvalidByteSliceSize(t *testing.T) {
	book := Book{}

	b := make([]byte, BookSize+10)
	err := book.UnmarshalBinary(b)
	if err == nil {
		t.Fatal("expected error to exist; got nil")
	}
	if err.Error() != "invalid slice size" {
		t.Fatalf(`expected error to be "invalid slice size"; got "%s"`, err.Error())
	}

	err = book.UnmarshalBinary(b[:BookSize-10])
	if err == nil {
		t.Fatal("expected error to exist; got nil")
	}
	if err.Error() != "invalid slice size" {
		t.Fatalf(`expected error to be "invalid slice size"; got "%s"`, err.Error())
	}
}
