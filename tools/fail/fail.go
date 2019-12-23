package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

func main() {
	var ta = NewAnalyze()
	ta.Load("./logs")
	fmt.Println(ta.WriteToFile("./result.json"))
}

type analyze struct {
	mu   sync.Mutex
	logs []string
}

func NewAnalyze() *analyze {
	var t = &analyze{}
	return t
}

func (this *analyze) Load(dir string) error {
	var fileInfo, err = os.Stat(dir)
	if err != nil {
		return err
	}

	var pathList []string

	if fileInfo.IsDir() {
		var file *os.File
		file, err = os.Open(dir)
		if err != nil {
			return err
		}

		var names []string
		names, err = file.Readdirnames(-1)

		file.Close()
		if err != nil {
			return err
		}

		for _, name := range names {
			var filePath = path.Join(dir, name)
			fileInfo, err = os.Stat(filePath)
			if err != nil {
				continue
			}

			if !fileInfo.IsDir() {
				pathList = append(pathList, filePath)
			}
		}
	} else {
		pathList = append(pathList, dir)
	}

	return this.LoadFiles(pathList...)
}

func (this *analyze) LoadFiles(files ...string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, file := range files {
		var f, err = os.OpenFile(file, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		err = this.load(f)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *analyze) load(r io.Reader) error {
	var reader = bufio.NewReader(r)
	var line []byte
	var err error

	var index = 0
	for {
		if line, _, err = reader.ReadLine(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if index == 0 {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
		}
		index++

		var sLine = strings.TrimSpace(string(line))

		// 如果是注释或者空行,则忽略
		if sLine == "" {
			continue
		}

		if strings.Contains(sLine, " Failed: ") {
			this.logs = append(this.logs, sLine)
		}
	}
	return nil
}

func (this *analyze) JSON() string {
	this.mu.Lock()
	defer this.mu.Unlock()

	bs, _ := json.Marshal(this.logs)
	return string(bs)
}

func (this *analyze) WriteToFile(file string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	bs, err := json.Marshal(this.logs)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_SYNC|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}

	var writer = bufio.NewWriter(f)
	if _, err = writer.Write(bs); err != nil {
		return err
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	return f.Close()
}
