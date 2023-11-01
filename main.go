package main

import (
	"fmt"
	"log"
	"os"
)

func writeBooks(books []Book) error {
	f, err := os.OpenFile("./data/data", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	bookBuf := BookBuffer{}

	for i := 0; i < len(books); i++ {
		bookBuf.SetFromBook(&books[i])
		if _, err := bookBuf.ToWriterAt(f, i); err != nil {
			return err
		}
	}

	return nil
}

func readBooks() (*[]Book, error) {
	f, err := os.OpenFile("./data/data", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	booksCount := TotalBookCount(stat)

	bookBuf := BookBuffer{}

	books := []Book{}
	for i := 0; i < int(booksCount); i++ {
		if _, err := bookBuf.FromReaderAt(f, i); err != nil {
			return nil, err
		}
		books = append(books, *bookBuf.ToBook())
	}

	return &books, nil
}

func main() {
	if err := os.MkdirAll("./data", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	booksToWrite := []Book{
		{
			Title:     "Game of Thrones",
			PageCount: 1012,
		},
		{
			Title:     "Harry Potter",
			PageCount: 1012,
		},
		{
			Title:     "The Lord of the Rings",
			PageCount: 1012,
		},
		{
			Title:     "The Little Prince",
			PageCount: 1012,
		},
	}

	if err := writeBooks(booksToWrite); err != nil {
		log.Fatal(err)
	}

	books, err := readBooks()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(*books); i++ {
		fmt.Println((*books)[i])
	}
}
