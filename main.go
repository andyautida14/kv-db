package main

import (
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
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

func writeIndices(indices []BookIndex) error {
	f, err := os.OpenFile("./data/index", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Truncate(0); err != nil {
		return err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	indexTable := NewBookIndexReadWriter(f)

	for _, item := range indices {
		fmt.Print("inserting index:", item)
		// if err := LinearInsert(&item, indexTable); err != nil {
		// 	return err
		// }
		if err := Insert(&item, indexTable); err != nil {
			return err
		}
	}

	return nil
}

func readIndexTable() error {
	f, err := os.OpenFile("./data/index", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	indexTable := NewBookIndexReadWriter(f)
	count, err := indexTable.Count()
	if err != nil {
		return err
	}

	for i := int64(0); i < count; i++ {
		index, err := indexTable.GetIndexByOffset(i)
		if err != nil {
			return err
		}
		fmt.Println("written item:", index, "offset:", i)
	}

	return nil
}

func readIndices(indices []BookIndex) error {
	f, err := os.OpenFile("./data/index", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	indexTable := NewBookIndexReadWriter(f)

	for _, index := range indices {
		fmt.Print("finding index:", index, " ")
		_, err := BinarySearch(index.Id, indexTable)
		if err != nil {
			return err
		}
	}

	return nil
}

func mainOld() {
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

func main() {
	var indices []BookIndex
	for i := 0; i < 20; i++ {
		indices = append(indices, BookIndex{
			Id:     uuid.New(),
			Offset: 0,
		})
	}

	if err := writeIndices(indices); err != nil {
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				fmt.Printf("%+s:%d\n", f, f)
			}
		}
		log.Fatal(err)
	}

	if err := readIndexTable(); err != nil {
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				fmt.Printf("%+s:%d\n", f, f)
			}
		}
		log.Fatal(err)
	}

	if err := readIndices(indices); err != nil {
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				fmt.Printf("%+s:%d\n", f, f)
			}
		}
		log.Fatal(err)
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
