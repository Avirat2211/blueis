package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {

	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {

	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return aof.file.Sync()
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	resp := NewResp(aof.file)

	for {
		value, err := resp.Read()
		if err == nil {
			callback(value)
			continue
		}
		if err == io.EOF {
			break
		}
		fmt.Println("AOF replay error:", err)
		continue
	}

	return nil
}

func handleExpireWrite(aof *Aof, args []Value) error {
	if len(args) == 2 {
		secondsInt, err := strconv.ParseInt(args[1].bulk, 10, 64)
		if err != nil {
			return err
		}
		expiryTime := time.Now().Unix() + secondsInt
		expireValue := Value{
			typ: "array",
			array: []Value{
				{typ: "bulk", bulk: "EXPIRESAT"},
				{typ: "bulk", bulk: args[0].bulk},
				{typ: "bulk", bulk: strconv.FormatInt(expiryTime, 10)},
			},
		}
		return aof.Write(expireValue)
	}
	return nil
}
