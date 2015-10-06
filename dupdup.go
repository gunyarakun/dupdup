package main

import (
	"github.com/codegangsta/cli"
//	"log"
	"os"
	"path/filepath"
	"crypto/md5"
	"errors"
	"io/ioutil"
	"sync"
	"runtime"
	"fmt"
	"bytes"
	"sort"
)

const DEFAULT_THRESHOLD_SIZE = 1024 * 1024

type FileInfoUniqueKey struct {
	size int64
	md5 [md5.Size]byte
}

type FileInfo struct {
	path string
	size int64
	md5 [md5.Size]byte
	err error
}

type FileInfoSlice []FileInfo

func (s FileInfoSlice) Len() int { return len(s) }
func (s FileInfoSlice) Less(i, j int) bool { return s[i].size > s[j].size || (s[i].size == s[j].size &&
bytes.Compare(s[i].md5[:], s[j].md5[:]) < 0) }
func (s FileInfoSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// walkFiles start a goroutine to walk the directory tree at root and send the
// path and size of each regular file on the FileInfo channel. It sends the result of the
// walk on the error channel. If done is closed, walkFiles abandons its work.
func walkFiles(done <-chan struct{}, root string, thresholdSize int64) (<-chan FileInfo, <-chan error) {
	var d [md5.Size]byte;
	fileinfos := make(chan FileInfo)
	errc := make(chan error, 1)

	s := map[int64]bool{}
	m := map[int64]FileInfo{}

	go func() {
		defer close(fileinfos)
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			size := info.Size()
			if size < thresholdSize {
				return nil
			}
			_, ok := s[size]
			if !ok {
				prei, ok := m[size]
				if ok {
					fileinfos <- prei
					s[size] = true
				} else {
					m[size] = FileInfo{path, size, d, nil}
					return nil
				}
			}

			select {
			case fileinfos <- FileInfo{path, size, d, nil}:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return fileinfos, errc
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

func findDuplicatedDigest(done <-chan struct{}, in <-chan FileInfo) FileInfoSlice{
	var result FileInfoSlice

	s := map[FileInfoUniqueKey]bool{}
	m := map[FileInfoUniqueKey]FileInfo{}

	for r := range in {
		key := FileInfoUniqueKey{r.size, r.md5}
		_, ok := s[key]
		if !ok {
			p, ok := m[key]
			if ok {
				result = append(result, p)
				s[key] = true
			} else {
				m[key] = r
				continue
			}
		}
		result = append(result, r)
	}

	return result
}

func findDuplicatedFile(root string, thresholdSize int64) {
	done := make(chan struct{})
	defer close(done)

	fileinfos, errc := walkFiles(done, root, thresholdSize)
	digests := runDigesters(done, fileinfos)
	results := findDuplicatedDigest(done, digests)

	sort.Sort(results)

	for _, r := range results {
		fmt.Printf("%s\t%d\t%032x\n", r.path, r.size, r.md5)
	}
	if err := <-errc; err != nil {
		return
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "dupdup"
	app.HideHelp = true
	app.Action = func(c *cli.Context) {
		findDuplicatedFile(c.Args()[0], DEFAULT_THRESHOLD_SIZE)
	}
	app.Run(os.Args)
}
