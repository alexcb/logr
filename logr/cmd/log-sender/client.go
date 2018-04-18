package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

type logr struct {
	addr     string
	msgs     []string
	lastSent time.Time
	lock     *sync.Mutex
}

func (s *logr) flush() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.msgs) == 0 {
		return
	}
	s.flushUnsafe()
}

func (s *logr) flushUnsafe() {
	var buf bytes.Buffer

	for _, msg := range s.msgs {
		binary.Write(&buf, binary.LittleEndian, uint64(len(msg)))
		buf.Write([]byte(msg))
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		fmt.Printf("err1: %v\n", err)
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Printf("err2: %v\n", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("err3: %v\n", err)
		return
	}

	s.msgs = nil
	s.lastSent = time.Now()
}

func (s *logr) send(msg string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.msgs = append(s.msgs, msg)
	if len(s.msgs) < 1000 && time.Since(s.lastSent) < time.Minute {
		return
	}

	s.flushUnsafe()
}

func main() {
	l := &logr{
		addr: "localhost:4000",
		lock: &sync.Mutex{},
	}
	i := 0
	for {
		l.send(fmt.Sprintf("hello %d", i))
		i++
		if (i % 100000) == 0 {
			fmt.Printf("%v: sent %d\n", time.Now(), i)
		}
	}
}
