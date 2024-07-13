package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	// "strconv"

	"github.com/coldstar-507/utils"
)

var (
	min = "0000000000000"
	max = "9999999999999"
)

// {kind}-{timestamp}-{unik}-{place}
// byte-int64-int32-int16
type timeId struct {
	kind      byte
	timestamp int64
	unik      uint32
	place     uint16
}

func makeTimeId(kind byte, place uint16) *timeId {
	return &timeId{
		kind:      kind,
		timestamp: utils.MakeTimestamp(),
		unik:      rand.Uint32(),
		place:     place,
	}
}

func (tid *timeId) toString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 1+8+4+2))
	binary.Write(buf, binary.BigEndian, tid.kind)
	binary.Write(buf, binary.BigEndian, tid.timestamp)
	binary.Write(buf, binary.BigEndian, tid.unik)
	binary.Write(buf, binary.BigEndian, tid.place)
	return hex.EncodeToString(buf.Bytes())
}

func tidFromString(stid string) *timeId {
	var (
		kind      byte
		timestamp int64
		unik      uint32
		place     uint16
	)

	buf, _ := hex.DecodeString(stid)
	rdr := bytes.NewReader(buf)
	binary.Read(rdr, binary.BigEndian, &kind)
	binary.Read(rdr, binary.BigEndian, &timestamp)
	binary.Read(rdr, binary.BigEndian, &unik)
	binary.Read(rdr, binary.BigEndian, &place)

	return &timeId{
		kind:      kind,
		timestamp: timestamp,
		unik:      unik,
		place:     place,
	}
}

func main() {
	aa := makeTimeId(0xbb, 0xffaa)
	aastr := aa.toString()
	fmt.Println(aastr)
	aa_ := tidFromString(aastr)
	fmt.Println(aa_.toString())
}
