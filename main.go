package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

const BOOK_TITLE_S = 4 * 32
const BOOK_S = BOOK_TITLE_S + 4

type Book struct {
	Title     string
	PageCount uint32
}

func writeBook(f *os.File, book *Book, off int) error {
	off_b := int64(BOOK_S * off)

	title_b := make([]byte, BOOK_TITLE_S)
	copy(title_b, []byte(book.Title))
	if _, err := f.WriteAt(title_b, off_b); err != nil {
		return err
	}

	pageCount_b := make([]byte, 4)
	binary.LittleEndian.PutUint32(pageCount_b, book.PageCount)
	_, err := f.WriteAt(pageCount_b, BOOK_TITLE_S+off_b)
	return err
}

func readBook(f *os.File, book *Book, off int) error {
	off_b := int64(BOOK_S * off)

	title_b := make([]byte, BOOK_TITLE_S)
	if _, err := f.ReadAt(title_b, off_b); err != nil {
		return err
	}

	pageCount_b := make([]byte, 4)
	if _, err := f.ReadAt(pageCount_b, BOOK_TITLE_S+off_b); err != nil {
		return err
	}

	book.Title = string(title_b)
	book.PageCount = binary.LittleEndian.Uint32(pageCount_b)

	return nil
}

func writeBooks(books *[]Book) error {
	f, err := os.OpenFile("./data/data", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 0; i < len(*books); i++ {
		if err := writeBook(f, &(*books)[i], i); err != nil {
			log.Fatal(err)
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
	booksCount := stat.Size() / BOOK_S

	books := []Book{}
	for i := 0; i < int(booksCount); i++ {
		book := Book{}
		if err := readBook(f, &book, i); err != nil {
			return nil, err
		}

		books = append(books, book)
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

	if err := writeBooks(&booksToWrite); err != nil {
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
