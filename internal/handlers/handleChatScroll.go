package handlers

import (
	"log"
	"net"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func handleChatScrollBefore(ref, rbuf []byte, conn net.Conn) {
	var limit = 50
	prefix := tools.ExtractIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()
	it.Seek(ref)
	rbuf[3] = 0x01
	for limit > 0 && it.Prev() {
		val := it.Value()
		me := flatgen.GetRootAsMessageEvent(val, 3)
		if me.Type() == 0x00 {
			limit--
		}
		if _, err := conn.Write(val); err != nil {
			log.Println("Error writing chat-scroll to conn:", err)
			break
		}
	}
	hasMore := limit == 0 && it.Prev()
	rbuf[0] = CHAT_SCROLL_DONE
	rbuf[1] = 0x01
	if hasMore {
		rbuf[2] = 0x01
	} else {
		rbuf[2] = 0x00
	}
	conn.Write(rbuf)
}

func handleChatScrollAfter(ref, rbuf []byte, conn net.Conn) {
	var limit = 50
	prefix := tools.ExtractIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()
	it.Seek(ref)
	rbuf[3] = 0x01
	for limit > 0 && it.Next() {
		val := it.Value()
		me := flatgen.GetRootAsMessageEvent(val, 3)
		if me.Type() == 0x00 {
			limit--
		}
		if _, err := conn.Write(val); err != nil {
			log.Println("Error writing chat-scroll to conn:", err)
			break
		}
	}
	hasMore := limit == 0 && it.Next()
	rbuf[0] = CHAT_SCROLL_DONE
	rbuf[1] = 0x01
	if hasMore {
		rbuf[2] = 0x01
	} else {
		rbuf[2] = 0x00
	}
	conn.Write(rbuf)
}

// func handleChatScroll4(before bool, ref, rbuf []byte, conn net.Conn) {
// 	var limit = 50
// 	prefix := tools.ExtractIdPrefix(ref)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	defer it.Release()
// 	it.Seek(ref)
// 	var itFunc func() bool
// 	if before {
// 		rbuf[3] = 0x01
// 		itFunc = it.Prev
// 	} else {
// 		rbuf[3] = 0x00
// 		itFunc = it.Next
// 	}
// 	for limit > 0 && itFunc() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		if _, err := conn.Write(val); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && itFunc()
// 	rbuf[0] = CHAT_SCROLL_DONE
// 	rbuf[1] = 0x01
// 	if hasMore {
// 		rbuf[2] = 0x01
// 	} else {
// 		rbuf[2] = 0x00
// 	}
// 	conn.Write(rbuf)
// }

// func handleChatScroll2(sc *flatgen.ChatScrollRequestT, cch chan []byte) {
// 	rbuf := make([]byte, 4)
// 	var limit = 50
// 	it := db.LV.NewIterator(&util.Range{Start: []byte(sc.ChatId)}, nil)
// 	defer it.Release()
// 	var itFunc func() bool
// 	if sc.Before {
// 		rbuf[3] = 0x01
// 		itFunc = it.Prev
// 	} else {
// 		rbuf[3] = 0x00
// 		itFunc = it.Next
// 	}
// 	for limit > 0 && itFunc() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		buf := make([]byte, len(val))
// 		copy(buf, val)
// 		cch <- buf
// 	}
// 	hasMore := limit == 0 && itFunc()
// 	rbuf[0] = CHAT_SCROLL_DONE
// 	rbuf[1] = 0x01
// 	if hasMore {
// 		rbuf[2] = 0x01
// 	} else {
// 		rbuf[2] = 0x00
// 	}
// 	cch <- rbuf
// }

// func handleChatScroll3(before bool, chatId []byte, limit int, conn net.Conn) bool {
// 	it := db.LDB.NewIterator(&util.Range{Start: chatId}, nil)
// 	defer it.Release()
// 	var buf = make([]byte, 4096)
// 	buf[0] = 0x00
// 	var itFunc func() bool
// 	if before {
// 		itFunc = it.Prev
// 	} else {
// 		itFunc = it.Next
// 	}
// 	for limit > 0 && itFunc() {
// 		val := it.Value()
// 		l := len(val)
// 		binary.BigEndian.PutUint16(buf[1:3], uint16(l))
// 		copy(buf[3:3+l], val)
// 		me := flatgen.GetRootAsMessageEvent(val, 0)
// 		if me.Type() == 0 {
// 			limit--
// 		}
// 		if _, err := conn.Write(buf[:3+l]); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	return limit == 0 && itFunc()
// }
