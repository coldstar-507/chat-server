package main

import (
	"fmt"
	"hash/fnv"
)

func short(b []byte) []byte {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum(nil)
}

func main() {
	b := &[6]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	fmt.Printf("%T\n", short(b[:]))
	fmt.Printf("%X\n", short(b[:]))
	fmt.Printf("%X\n", b[:])

}
