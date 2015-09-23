package main

import (
	"github.com/codegangsta/cli"
	"log"
	"os"
	"filepath"
	"md5"
)

type FileInfo struct {
	path string
	size int64
	md5 [md5.Size]byte
}

// walkFiles start a goroutine to walk the directory tree at root and send the
// path and size of each regular file on the FileInfo channel. It sends the result of the
// walk on the error channel. If done is closed, walkFiles abandons its work.
func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
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
			case fileinfos <- FileInfo{path, info.Size(),nil}:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return fileinfos, errc
}

func FindDuplicatedFile(root string) (map[string]FileInfo, error) {

}

func main() {
	app := cli.NewApp()
	app.Name = "dupdup"
	app.HideHelp = true
	app.Action = func(c *cli.Context) {
		log.Println(c.Args())
	}
	app.Run(os.Args)
}
