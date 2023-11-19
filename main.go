package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

func main() {
	collection, err := NewBookCollection("./data")
	if err != nil {
		log.Fatal(err)
	}

	if err := collection.Reset(); err != nil {
		log.Fatal(err)
	}

	keys := []uuid.UUID{
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

	for i := 0; i < 4; i++ {
		if err := collection.Put(keys[i], &books[i]); err != nil {
			log.Fatal(err)
		}

		fmt.Println("saved book:", keys[i], books[i])
	}

	for _, key := range keys {
		book, err := collection.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("retrieved book:", key, book)
	}
}
