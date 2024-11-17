package handlers

import (
	"encoding/binary"
	"errors"
	"log"
	"net"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
)

func StartBoostServer() {
	listener, err := net.Listen("tcp", ":11003")
	utils.Panic(err, "StartBoostServer(): error on net.Listen")
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("StartBoostServer(): error accepting connection:", err)
		} else {
			log.Println("StartBoostServer(): new boost connection:",
				conn.LocalAddr())
		}
		go HandleBoostConn(conn)
	}
}

func HandleBoostConn(conn net.Conn) {
	defer conn.Close()

	var (
		t           byte
		msgLen      uint16
		boostTagLen uint16
		nBoosts     uint32
	)

	err0 := binary.Read(conn, binary.BigEndian, &t)
	err1 := binary.Read(conn, binary.BigEndian, &msgLen)
	msgBuf := make([]byte, msgLen)
	err2 := binary.Read(conn, binary.BigEndian, msgBuf)
	err3 := binary.Read(conn, binary.BigEndian, &boostTagLen)
	err4 := binary.Read(conn, binary.BigEndian, &nBoosts)
	if err := errors.Join(err0, err1, err2, err3, err4); err != nil {
		log.Println("HandleBoostConn: error reading request:\n\t", err)
	}

	log.Printf(`HandleBoostConn:
t           : %x
msgLen      : %d
boostTagLen : %d
nBoosts     : %d
`, t, msgLen, boostTagLen, nBoosts)

	if t != 0x88 {
		log.Printf("HandleBoostConn: wrong t byte:%x\n", t)
	}

	msg := flatgen.GetRootAsMessageEvent(msgBuf, 0)
	cid := msg.ChatId(nil)
	cid.MutateTimestamp(utils.MakeTimestamp())

	boostMsgId := utils.MakeRawMsgId(cid)

	if err := db.LV.Put(boostMsgId, msgBuf, nil); err != nil {
		log.Println("HandleBoostConn: error writing boost message:\n\t", err)
		return
	}

	for range nBoosts {
		if _, err := conn.Read(msgBuf[:boostTagLen]); err != nil {
			log.Println("HandleBoostConn: connection error:\n\t",
				err, "\n\tclosing conn")

			return
		} else if err := db.LV.Put(msgBuf[:boostTagLen], boostMsgId, nil); err != nil {
			log.Println("HandleBoostConn: error writing tag:", err, "continuing")
			continue
		}
		log.Printf("HandleBoostConn: wrote %x : %x\n", msgBuf[:boostTagLen], boostMsgId)
	}
	conn.Write([]byte{0x88})
}
