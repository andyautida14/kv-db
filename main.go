package main

import (
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
)

func main() {
	if err := os.MkdirAll("./data/book", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	bookStorage, err := NewStorage("./data/book/data", uint16(BookSize))
	if err != nil {
		log.Fatal(err)
	}

	if err := bookStorage.Reset(); err != nil {
		log.Fatal(err)
	}

	keyStorage, err := NewStorage("./data/book/keys", uint16(KeySize))
	if err != nil {
		log.Fatal(err)
	}

	if err := keyStorage.Reset(); err != nil {
		log.Fatal(err)
	}

	keyIndexer := NewKeyIndexer(keyStorage)

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

	keys := []Key{
		{
			Id:     uuid.New(),
			Offset: 0,
		},
		{
			Id:     uuid.New(),
			Offset: 1,
		},
		{
			Id:     uuid.New(),
			Offset: 2,
		},
		{
			Id:     uuid.New(),
			Offset: 3,
		},
	}

	for i, book := range books {
		b, err := book.MarshalBinary()
		if err != nil {
			log.Fatal(err)
		}

		if _, err := bookStorage.WriteOffset(b, int64(i)); err != nil {
			log.Fatal(err)
		}

		fmt.Println("written book:", i, book)
	}

	book := Book{}
	var b [BookSize]byte
	for i := 0; i < len(books); i++ {
		clear(b[:])

		if _, err := bookStorage.ReadOffset(b[:], int64(i)); err != nil {
			log.Fatal(err)
		}

		if err := book.UnmarshalBinary(b[:]); err != nil {
			log.Fatal(err)
		}

		fmt.Println("read book:", book)
	}

	for _, key := range keys {
		if _, err := keyIndexer.Insert(&key); err != nil {
			log.Fatal(err)
		}

		fmt.Println("written key:", key)
	}

	key := Key{}
	var k [KeySize]byte
	for i := 0; i < len(keys); i++ {
		clear(k[:])

		if _, err := keyStorage.ReadOffset(k[:], int64(i)); err != nil {
			log.Fatal(err)
		}

		if err := key.UnmarshalBinary(k[:]); err != nil {
			log.Fatal(err)
		}

		fmt.Println("read key:", key)
	}
}
