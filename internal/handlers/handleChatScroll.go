package handlers

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/utils"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// func handleChatScrollBeforeSync2(ref []byte, c *client2) {
// 	var limit = 50
// 	prefix := utils.RootPrefixFromRawMsg(ref)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	log.Printf("ref:\n%x\nroot prefix:\n%x\n", ref, prefix)
// 	defer it.Release()
// 	it.Seek(ref)
// 	for limit > 0 && it.Prev() {
// 		key := it.Key()
// 		if key[len(key)-1] == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll before: %x\n", it.Key())
// 		if _, err := c.conn.Write(it.Value()); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Prev()
// 	c.rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(c.rbuf[1:3], 2)
// 	if hasMore {
// 		c.rbuf[3] = 0x01
// 	} else {
// 		c.rbuf[3] = 0x00
// 	}
// 	// before byte (true)
// 	c.rbuf[4] = 0x01
// 	log.Println("rbuf len:", len(c.rbuf[:5]))
// 	c.conn.Write(c.rbuf[:5])
// }

// func handleChatScrollAfterSync2(ref []byte, c *client2) {
// 	var limit = 50
// 	prefix := utils.RootPrefixFromRawMsg(ref)
// 	log.Printf("ref:\n%x\nroot prefix:\n%x\n", ref, prefix)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	defer it.Release()
// 	for ok := it.Seek(ref); limit > 0 && ok; ok = it.Next() {
// 		key := it.Key()
// 		if key[len(key)-1] == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll after: %x\n", it.Key())
// 		if _, err := c.conn.Write(it.Value()); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Next()
// 	c.rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(c.rbuf[1:3], 2)
// 	if hasMore {
// 		c.rbuf[3] = 0x01
// 	} else {
// 		c.rbuf[3] = 0x00
// 	}
// 	// before byte (false)
// 	c.rbuf[4] = 0x00
// 	log.Println("rbuf len:", len(c.rbuf[:5]))
// 	c.conn.Write(c.rbuf[:5])
// }

func handleChatScrollBeforeSync3(ref []byte, c *client, limit uint16) {
	// var limit = 50
	prefix := utils.MsgIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	log.Printf("ref:\n%x\nroot prefix:\n%x\n", ref, prefix)
	defer it.Release()
	it.Seek(ref)
	// Seek returns greater or equal to ref
	// anyhow, we don't want it
	for limit > 0 && it.Prev() {
		key := it.Key()
		if key[len(key)-1] == 0x00 {
			limit--
		}
		log.Printf("yielding in scroll before: %x\n", it.Key())
		if _, err := c.conn.Write(it.Value()); err != nil {
			log.Println("Error writing chat-scroll to conn:", err)
			break
		}
	}

	hasMore := limit == 0 && it.Prev()

	payloadLen := 2 + utils.RAW_ROOT_ID_LEN
	fullResLen := 1 + 2 + payloadLen

	c.rbuf[0] = CHAT_SCROLL_DONE
	binary.BigEndian.PutUint16(c.rbuf[1:3], uint16(payloadLen))
	if hasMore {
		c.rbuf[3] = 0x01
	} else {
		c.rbuf[3] = 0x00
	}
	// before byte (true)
	c.rbuf[4] = 0x01
	copy(c.rbuf[5:], prefix[1:]) // remove msgId prefix, so we only have root
	log.Printf(`
handleChatScrollBeforeSync3:
c.rbuf[:fullResLen] = %X
prefix              = %X
prefix[1:]          = %X
`, c.rbuf[:fullResLen], prefix, prefix[1:])
	log.Printf("SCROLL END: WRITING\n%X\n", c.rbuf[:fullResLen])
	c.conn.Write(c.rbuf[:fullResLen])
}

func handleChatScrollAfterSync3(ref []byte, c *client, limit uint16) {
	prefix := utils.MsgIdPrefix(ref)
	log.Printf("ref:\n%x\nroot prefix:\n%x\n", ref, prefix)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	handleIt := func(it iterator.Iterator) error {
		key := it.Key()
		// MsgEvent ids have suffix, 0x00 is a chat
		if key[len(key)-1] == 0x00 {
			limit--
		}
		log.Printf("yielding in scroll after: %x\n", it.Key())
		if _, err := c.conn.Write(it.Value()); err != nil {
			log.Println("Error writing chat-scroll to conn:", err)
			return err
		}
		return nil
	}

	// special case for after, because it can be ref but
	// we don't want to handle ref, since it's been handled already
	if ok := it.Seek(ref); ok && !bytes.Equal(ref, it.Key()) {
		handleIt(it)
	}

	for limit > 0 && it.Next() {
		if err := handleIt(it); err != nil {
			break
		}
	}

	hasMore := limit == 0 && it.Next()

	payloadLen := 2 + utils.RAW_ROOT_ID_LEN
	fullResLen := 1 + 2 + payloadLen

	c.rbuf[0] = CHAT_SCROLL_DONE
	binary.BigEndian.PutUint16(c.rbuf[1:3], uint16(payloadLen))
	if hasMore {
		c.rbuf[3] = 0x01
	} else {
		c.rbuf[3] = 0x00
	}
	// before byte (true)
	c.rbuf[4] = 0x00
	copy(c.rbuf[5:], prefix[1:]) // remove msgId prefix, so we only have root
	log.Printf(`
handleChatScrollAfterSync3:
c.rbuf[:fullResLen] = %X
prefix              = %X
prefix[1:]          = %X
`, c.rbuf[:fullResLen], prefix, prefix[1:])
	log.Printf("SCROLL END: WRITING\n%X\n", c.rbuf[:fullResLen])
	c.conn.Write(c.rbuf[:fullResLen])
}

// func handleChatScrollBefore(ref, rbuf []byte, conn net.Conn) {
// 	var limit = 50
// 	prefix := utils.RootPrefixRaw(ref) // tools.ExtractIdPrefix(ref)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	log.Printf("scroll prefix: %x\n", prefix)
// 	defer it.Release()
// 	it.Seek(ref)
// 	for limit > 0 && it.Prev() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll before: %x\n", it.Key())
// 		if _, err := conn.Write(val); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Prev()
// 	rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(rbuf[1:3], 1)
// 	if hasMore {
// 		rbuf[3] = 0x01
// 	} else {
// 		rbuf[3] = 0x00
// 	}
// 	conn.Write(rbuf)
// }

// func handleChatScrollAfter(ref, rbuf []byte, conn net.Conn) {
// 	var limit = 50
// 	prefix := utils.RootPrefixRaw(ref) // prefix := tools.ExtractIdPrefix(ref)
// 	log.Printf("scroll prefix: %x\n", prefix)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	defer it.Release()
// 	for ok := it.Seek(ref); limit > 0 && ok; ok = it.Next() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll after: %x\n", it.Key())
// 		if _, err := conn.Write(val); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Next()
// 	rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(rbuf[1:3], 1)
// 	if hasMore {
// 		rbuf[3] = 0x01
// 	} else {
// 		rbuf[3] = 0x00
// 	}
// 	conn.Write(rbuf)
// }

// func handleChatScrollBeforeSync(ref []byte, c *client2) {
// 	var limit = 50
// 	prefix := utils.RootPrefixRaw(ref) // tools.ExtractIdPrefix(ref)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	log.Printf("scroll prefix: %x\n", prefix)
// 	defer it.Release()
// 	it.Seek(ref)
// 	for limit > 0 && it.Prev() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll before: %x\n", it.Key())
// 		if _, err := c.conn.Write(val); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Prev()
// 	c.rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(c.rbuf[1:3], 1)
// 	if hasMore {
// 		c.rbuf[3] = 0x01
// 	} else {
// 		c.rbuf[3] = 0x00
// 	}
// 	c.conn.Write(c.rbuf)
// }

// func handleChatScrollAfterSync(ref []byte, c *client2) {
// 	var limit = 50
// 	prefix := utils.RootPrefixRaw(ref) // prefix := tools.ExtractIdPrefix(ref)
// 	log.Printf("scroll prefix: %x\n", prefix)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	defer it.Release()
// 	for ok := it.Seek(ref); limit > 0 && ok; ok = it.Next() {
// 		val := it.Value()
// 		me := flatgen.GetRootAsMessageEvent(val, 3)
// 		if me.Type() == 0x00 {
// 			limit--
// 		}
// 		log.Printf("yielding in scroll after: %x\n", it.Key())
// 		if _, err := c.conn.Write(val); err != nil {
// 			log.Println("Error writing chat-scroll to conn:", err)
// 			break
// 		}
// 	}
// 	hasMore := limit == 0 && it.Next()
// 	c.rbuf[0] = CHAT_SCROLL_DONE
// 	binary.BigEndian.PutUint16(c.rbuf[1:3], 1)
// 	if hasMore {
// 		c.rbuf[3] = 0x01
// 	} else {
// 		c.rbuf[3] = 0x00
// 	}
// 	c.conn.Write(c.rbuf)
// }
