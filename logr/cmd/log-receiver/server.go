package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"golang.org/x/sync/semaphore"
)

func writeLoop(fp *os.File, messages <-chan []*string) {
	for {
		select {
		case msg := <-messages:
			for _, s := range msg {
				fp.WriteString(strconv.QuoteToASCII(*s))
				fp.WriteString("\n")
			}
		case <-time.After(time.Second):
		}
		fp.Sync()
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:4000")
	if err != nil {
		log.Fatal(err)
	}

	fp, err := os.Create("msg.log")
	if err != nil {
		log.Fatal(err)
	}

	messages := make(chan []*string, 10)
	go writeLoop(fp, messages)

	sem := semaphore.NewWeighted(int64(50))
	ctx := context.Background()

	defer l.Close()
	for {

		if err := sem.Acquire(ctx, 1); err != nil {
			panic(err)
		}

		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("accept err: %v\n", err)
			continue
		}
		go func(c net.Conn) {
			defer func() {
				c.Close()
				defer sem.Release(1)
			}()

			data := []*string{}
			for {
				var n uint64
				//fmt.Printf("reading len\n")
				err = binary.Read(c, binary.LittleEndian, &n)
				if err != nil {
					if err != io.EOF {
						fmt.Printf("err: %v\n", err)
					}
					break
				}

				//fmt.Printf("reading bytes: %d\n", n)
				s := make([]byte, n)
				nn, err := io.ReadFull(c, s)
				if err != nil {
					fmt.Printf("err: %v\n", err)
					break
				}
				if int(nn) != int(n) {
					fmt.Printf("want=%d got=%d\n", n, nn)
					break
				}
				//fmt.Printf("sending: %s\n", string(s))
				ss := string(s)
				data = append(data, &ss)
			}

			messages <- data
		}(conn)
	}
}
