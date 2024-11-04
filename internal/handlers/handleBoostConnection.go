package handlers

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"net"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
)

func StartBoostServer() {
	listener, err := net.Listen("tcp", ":11003")
	utils.Panic(err, "startChatServer error on net.Listen")
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
		} else {
			log.Println("new boost connection:", conn.LocalAddr())
		}
		go HandleBoostConn(conn)
	}
}

func HandleBoostConn(conn net.Conn) {
	defer conn.Close()
	b := make([]byte, 2*1024)
	_, err := conn.Read(b[:1])
	if err != nil {
		log.Println("HandleBoostConn: error reading first byte:", err)
		return
	}

	if b[0] != 0x88 {
		log.Println("HandleBoostConn: error first by should be 0x88")
		return
	}

	if _, err = conn.Read(b[:2]); err != nil {
		log.Println("HandleBoostConn: error reading msglen bytes:", err)
		return
	}

	msgLen := binary.BigEndian.Uint16(b[:2])
	if int(msgLen) > cap(b) {
		b = make([]byte, msgLen)
	}

	if _, err = conn.Read(b[:msgLen]); err != nil {
		log.Println("HandleBoostConn: error reading msg bytes:", err)
		return
	}

	msg := flatgen.GetRootAsMessageEvent(b[:msgLen], 0)
	cid := msg.ChatId(nil)
	cid.MutateTimestamp(utils.MakeTimestamp())
	// id.MutatePlace(Place)

	boostMsgId := utils.MakeRawMsgId(cid)
	// idb := tools.MsgIdPool.Get()
	// idbcap := cap(idb)
	// defer tools.MsgIdPool.Put(idb)
	// idbuf := bytes.NewBuffer(idb[:0])
	// utils.WriteMsgId(idbuf, id)
	// if idbuf.Cap() != idbcap {
	// 	panic("should not have an allocation here")
	// }

	// strId := utils.FastStringToBytes(utils.StrMsgId(id))
	err = db.LV.Put(boostMsgId, b[:msgLen], nil)
	if err != nil {
		log.Println("HandleBoostConn: error writing boost message:", err)
		return
	}

	var tagLen uint16
	for {
		if _, err = conn.Read(b[:2]); err != nil {
			if err == io.EOF {
				log.Println("HandleBoostConn: boosts all written closing conn")
				return
			} else {
				log.Println("HandleBoostConn: connection error:",
					err, "closing conn")
				return
			}
		}
		tagLen = binary.BigEndian.Uint16(b[:2])
		if _, err = conn.Read(b[:tagLen]); err != nil {
			log.Println("HandleBoostConn: connection error:", err, "closing conn")
			return
		}

		if err = db.LV.Put(b[:tagLen], boostMsgId, nil); err != nil {
			log.Println("HandleBoostConn: error writing tag:", err, "continuing")
			continue
		} else {
			strTag := hex.EncodeToString(b[:tagLen])
			strMsgId := hex.EncodeToString(boostMsgId)
			log.Printf("HandleBoostConn: wrote %s : %s\n", strTag, strMsgId)
		}
	}
}
