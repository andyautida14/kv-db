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

	bookRw := NewBookReadWriter(f, 0)

	bookBuf := BookBuffer{}

	for i := 0; i < len(books); i++ {
		bookBuf.SetFromBook(&books[i])
		bookRw.SetOffset(int64(i))
		if _, err := bookBuf.WriteTo(bookRw); err != nil {
			return err
		}
	}

	return nil
}

func updateBook(off int, title string, pageCount uint32) error {
	f, err := os.OpenFile("./data/data", os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	bookRw := NewBookReadWriter(f, int64(off))

	oldTitle, err := bookRw.Title()
	if err != nil {
		return err
	}
	fmt.Println("title before update:", oldTitle)

	oldPageCount, err := bookRw.PageCount()
	if err != nil {
		return err
	}
	fmt.Println("page count before update:", oldPageCount)

	if err := bookRw.SetTitle(title); err != nil {
		return err
	}

	return bookRw.SetPageCount(pageCount)
}

func readBooks() (*[]Book, error) {
	f, err := os.OpenFile("./data/data", os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bookRw := NewBookReadWriter(f, 0)
	booksCount, err := bookRw.Count()
	if err != nil {
		return nil, err
	}

	bookBuf := BookBuffer{}

	books := []Book{}
	for i := 0; i < int(booksCount); i++ {
		bookRw.SetOffset(int64(i))
		if _, err := bookBuf.ReadFrom(bookRw); err != nil {
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
			PageCount: 1024,
		},
		{
			Title:     "Harry Potter",
			PageCount: 1024,
		},
		{
			Title:     "The Lord of the Rings",
			PageCount: 1024,
		},
		{
			Title:     "The Little Prince",
			PageCount: 1024,
		},
	}

	if err := writeBooks(booksToWrite); err != nil {
		log.Fatal(err)
	}

	newTitle := "Harry Potter and the Order of the Phoenix"
	if err := updateBook(1, newTitle, 2048); err != nil {
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
