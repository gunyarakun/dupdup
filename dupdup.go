package main

import (
	"github.com/codegangsta/cli"
	"log"
	"os"
	"path/filepath"
	"crypto/md5"
	"errors"
	"io/ioutil"
	"sync"
	"runtime"
)

type FileInfo struct {
	path string
	size int64
	md5 [md5.Size]byte
	err error
}

// walkFiles start a goroutine to walk the directory tree at root and send the
// path and size of each regular file on the FileInfo channel. It sends the result of the
// walk on the error channel. If done is closed, walkFiles abandons its work.
func walkFiles(done <-chan struct{}, root string) (<-chan FileInfo, <-chan error) {
	var d [md5.Size]byte;
	fileinfos := make(chan FileInfo)
	errc := make(chan error, 1)
	go func() {
		defer close(fileinfos)
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}

			select {
			case fileinfos <- FileInfo{path, info.Size(), d, nil}:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return fileinfos, errc
}

func filterSameSize(done <-chan struct{}, in <-chan FileInfo) (<-chan FileInfo) {
	out := make(chan FileInfo)
	s := map[int64]bool{}
	m := map[int64]FileInfo{}
	go func() {
		for {
			i := <-in
			log.Printf(`path: %s\n`, i.path)
			_, ok := s[i.size]
			if ok {
				out <- i
			} else {
				prei, ok := m[i.size]
				if ok {
					out <- prei
					out <- i
					s[i.size] = true
				} else {
					m[i.size] = i
				}
			}
		}
	}()
	return out
}

func digester(done <-chan struct{}, infos <-chan FileInfo, c chan<- FileInfo) {
	for info := range infos {
		data, err := ioutil.ReadFile(info.path)
		select {
		case c <- FileInfo{info.path, info.size, md5.Sum(data), err}:
		case <-done:
			return
		}
	}
}

func runDigesters(done <-chan struct{}, in <-chan FileInfo) (<-chan FileInfo){
	c := make(chan FileInfo)
	var wg sync.WaitGroup
	numDigesters := runtime.GOMAXPROCS(0)
	wg.Add(numDigesters)
	for i := 0; i < numDigesters; i++ {
		go func() {
			digester(done, in, c)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	return c
}

func findDuplicatedFile(root string) {
	done := make(chan struct{})
	defer close(done)

	fileinfos, errc := walkFiles(done, root)
	for r := range fileinfos {
		log.Printf(`%s\t%d\n`, r.path, r.size)
	}
	/*
	sfileinfos := filterSameSize(done, fileinfos)
	result := runDigesters(done, sfileinfos)

	for r := range result {
		log.Printf(`%s\t%d\t%x\n`, r.path, r.size, r.md5)
	}
	*/
	if err := <-errc; err != nil {
		return
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "dupdup"
	app.HideHelp = true
	app.Action = func(c *cli.Context) {
		findDuplicatedFile(c.Args()[0])
	}
	app.Run(os.Args)
}
