package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
)

func main() {
	var ta = NewAnalyze()
	ta.Load("./logs")
	fmt.Println(ta.UnClosedToFile("./result.json"))
}

// --------------------------------------------------------------------------------
var beginTxRegexp = regexp.MustCompile(`Transaction \[(?P<txId>[^]]+)\] Begin`)
var commitTxRegexp = regexp.MustCompile(`Transaction \[(?P<txId>[^]]+)\] Commit`)
var rollbackTxRegexp = regexp.MustCompile(`Transaction \[(?P<txId>[^]]+)\] Rollback`)

func beginTx(s string) (id string) {
	var rList = beginTxRegexp.FindStringSubmatch(s)
	if len(rList) >= 2 {
		id = rList[1]
	}
	return id
}

func commitTx(s string) (id string) {
	var rList = commitTxRegexp.FindStringSubmatch(s)
	if len(rList) >= 2 {
		id = rList[1]
	}
	return id
}

func rollbackTx(s string) (id string) {
	var rList = rollbackTxRegexp.FindStringSubmatch(s)
	if len(rList) >= 2 {
		id = rList[1]
	}
	return id
}

// --------------------------------------------------------------------------------
type analyze struct {
	mu     sync.Mutex
	txList map[string]*txInfo
}

func NewAnalyze() *analyze {
	var t = &analyze{}
	t.txList = make(map[string]*txInfo)
	return t
}

type txInfo struct {
	TxId     string `json:"id"`
	Begin    string `json:"begin,omitempty"`
	Commit   string `json:"commit,omitempty"`
	Rollback string `json:"rollback,omitempty"`
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

		var id string
		if id = beginTx(sLine); id != "" {
			var tInfo = this.getTxInfo(id)
			tInfo.Begin = sLine
			continue
		}
		if id = commitTx(sLine); id != "" {
			var tInfo = this.getTxInfo(id)
			tInfo.Commit = sLine
			continue
		}
		if id = rollbackTx(sLine); id != "" {
			var tInfo = this.getTxInfo(id)
			tInfo.Rollback = sLine
			continue
		}
	}
	return nil
}

func (this *analyze) getTxInfo(id string) *txInfo {
	var tInfo = this.txList[id]
	if tInfo == nil {
		tInfo = &txInfo{}
		tInfo.TxId = id
		this.txList[id] = tInfo
	}
	return tInfo
}

func (this *analyze) JSON() string {
	this.mu.Lock()
	defer this.mu.Unlock()

	bs, _ := json.Marshal(this.txList)
	return string(bs)
}

func (this *analyze) WriteToFile(file string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	bs, err := json.Marshal(this.txList)
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

func (this *analyze) UnClosedToFile(file string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	var ucList = make(map[string]*txInfo)
	for id, txInfo := range this.txList {
		if txInfo.Rollback == "" && txInfo.Commit == "" {
			ucList[id] = txInfo
		}
	}

	bs, err := json.Marshal(ucList)
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
