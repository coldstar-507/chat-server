package tools

import (
	// "bytes"
	// "encoding/binary"
	// "bytes"
	// "encoding/binary"
	// "encoding/hex"
	"math/rand"

	// "strconv"
	// "unsafe"

	"sync"

	// "github.com/coldstar-507/flatgen"
	// "github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils/id_utils"
	"github.com/coldstar-507/utils/utils"
)

var (
	FourKbPool = NewBytePool(4096)
	OneKbPool  = NewBytePool(1024)
	MediumPool = NewBytePool(256)
	RootPool   = NewBytePool(id_utils.RAW_ROOT_ID_LEN)
	PushIdPool = NewBytePool(id_utils.RAW_PUSH_ID_LEN)
	MsgIdPool  = NewBytePool(id_utils.RAW_MSG_ID_LEN)
	SmallPool  = NewBytePool(128)
	TinyPool   = NewBytePool(4)
)

// BytePool is a simple byte slice pool.
type bytePool struct {
	pool sync.Pool
}

// NewBytePool creates a new byte slice pool.
func NewBytePool(n int) *bytePool {
	return &bytePool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		},
	}
}

// Get retrieves a byte slice from the pool.
func (p *bytePool) Get() []byte {
	return p.pool.Get().([]byte)
}

// Put returns a byte slice to the pool.
func (p *bytePool) Put(buf []byte) {
	p.pool.Put(buf)
}

// The prefix will be a root or a device
// both are unique point of interests for sorting
// thus very useful for sorting data in leveldb
// func ExtractIdPrefix(id []byte) []byte {
// 	return ExtractNthIdPrefix(id, 1)
// }

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

// peak go? // that's crazy
// when n is big, extracts the biggest prefix, should not use n bigger than the
// amount of prefix though cause func has no stop for that
// n <= 0 also returns the first prefix
// func ExtractNthIdPrefix(id []byte, n int) []byte {
// 	for ix := bytes.IndexByte(id, '-'); ix != -1; ix = bytes.IndexByte(id[ix+1:], '-') + ix + 1 {
// 		if n--; n <= 0 {
// 			return id[:ix]
// 		}
// 	}
// 	return id
// }

// func MsgIdValue(id *flatgen.MessageId, b []byte) {
// 	buf := bytes.NewBuffer(b)
// 	binary.Write(buf, binary.BigEndian, id.Timestamp())
// }

// // root-timestamp-unik-place-suffix
// func WriteMsgIdValue(tid *flatgen.MessageId, buf []byte) int {
// 	var k int
// 	writeHyphen := func() { buf[k] = '-'; k += 1 }
// 	k += copy(buf, tid.Root())
// 	writeHyphen()
// 	k += copy(buf[k:], strconv.Itoa(int(tid.Timestamp())))
// 	writeHyphen()
// 	k += copy(buf[k:], tid.Unik())
// 	writeHyphen()
// 	k += copy(buf[k:], tid.Place())
// 	writeHyphen()
// 	k += copy(buf[k:], tid.Suffix())
// 	return k
// }

// func FastBytesToString(b []byte) string {
// 	return unsafe.String(unsafe.SliceData(b), len(b))
// }

// func FastStringToBytes(s string) []byte {
// 	return unsafe.Slice(unsafe.StringData(s), len(s))
// }

// prefix will be {userSecret-devId}
// but we bundle it as one to avoid unecessary memory allocation
// prefix-timestamp-unik-place-suffix
// func WritePushIdValue(pid *flatgen.PushId, buf []byte) int {
// 	var k int
// 	writeHyphen := func() { buf[k] = '-'; k += 1 }
// 	k += copy(buf, pid.Prefix())
// 	writeHyphen()
// 	k += copy(buf[k:], strconv.Itoa(int(pid.Timestamp())))
// 	writeHyphen()
// 	k += copy(buf[k:], pid.Unik())
// 	writeHyphen()
// 	k += copy(buf[k:], pid.Place())
// 	writeHyphen()
// 	k += copy(buf[k:], pid.Suffix())
// 	return k
// }

// func MsgIdValue(mid *flatgen.MessageId, buf []byte) []byte {
// 	bbuf := bytes.NewBuffer(buf[:0])
// 	bbuf.Write(mid.Root())
// 	bbuf.WriteRune('-')
// 	bbuf.WriteString(strconv.Itoa(int(mid.Timestamp())))
// 	bbuf.WriteRune('-')
// 	bbuf.Write(mid.Unik())
// 	bbuf.WriteRune('-')
// 	bbuf.Write(mid.Place())
// 	bbuf.WriteRune('-')
// 	bbuf.Write(mid.Suffix())
// 	return bbuf.Bytes()
// }

// func timeIdValue(tid *flatgen.TimeId, buf []byte) []byte {
// 	bbuf := bytes.NewBuffer(buf[:0])
// 	bbuf.WriteString(strconv.Itoa(int(tid.Timestamp())))
// 	bbuf.WriteRune('-')
// 	bbuf.Write(tid.Unik())
// 	bbuf.WriteRune('-')
// 	bbuf.Write(tid.Place())
// 	return bbuf.Bytes()
// }
