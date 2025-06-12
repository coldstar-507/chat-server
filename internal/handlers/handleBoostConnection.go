package handlers

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils/id_utils"
	"github.com/coldstar-507/utils/utils"
)

var (
	n_boost_writer uint32
	boost_mans     []*BoostsManager
)

func loadBoostManConf() {
	if v := os.Getenv("N_BOOST_MAN"); len(v) == 0 {
		panic("Undefined: N_BOOST_MAN")
	} else if i, err := strconv.Atoi(v); err != nil {
		panic(err)
	} else if e := uint32(i); e == 0 {
		panic("invalid N_BOOST_MAN value")
	} else {
		n_boost_writer = e
	}
	log.Println("loadBoostManConf: n_boost_writer:", n_boost_writer)
}

func idf_(buf []byte) uint32 {
	h := fnv.New32()
	h.Write(buf)
	return h.Sum32() % n_man
}

func getBoostMan(buf []byte) *BoostsManager {
	i := idf_(buf)
	return boost_mans[i]
}

type boostReq struct {
	boost *flatgen.Booster
	res   chan struct{}
}

type BoostsManager struct {
	i       uint32
	boostCh chan *boostReq
}

func StartBoostServer() {
	listener, err := net.Listen("tcp", ":11003")
	utils.Panic(err, "startBoostServer(): error on net.Listen")
	defer listener.Close()
	loadBoostManConf()
	boost_mans = make([]*BoostsManager, n_boost_writer)
	for i := range n_boost_writer {
		bm := &BoostsManager{i: i, boostCh: make(chan *boostReq, 128)}
		go bm.Run()
		boost_mans[i] = bm
	}
	for {
		log.Println("StartBoostServer: listening for connections")
		conn, err := listener.Accept()
		if err != nil {
			log.Println("StartBoostServer(): error accepting conn:", err)
			continue
		} else {
			log.Println("StartBoostServer(): new boost conn:", conn.LocalAddr())
		}
		go HandleBoostConn(conn)
	}
}

// do we need that?
// it is safer, but seems potentially slower to put all boosts writes
// on the same routine

// the case write(boost1), read(until to boost1), write(boost0)
// with boost0 ts < boost1 ts
// is possible, which is why we need to sync boost writes.
func (bm *BoostsManager) Run() {
	const stmt = `INSERT INTO db_one.boosts (node_id, ts, nonce, booster)
                      VALUES (?, ?, ?, ?)`
	for {
		boostr := <-bm.boostCh
		ts := utils.MakeTimestamp()
		nonce := id_utils.RandU32()
		boost := boostr.boost
		if mutated := boost.MutateTimestamp(ts); !mutated {
			log.Println("WARNING: BoostManager: could not mutate timestamp")
			continue
		}
		q := db.Scy.Query(stmt, boost.RawNodeIdBytes(), ts, nonce, boost.Table().Bytes)
		if err := q.Exec(); err != nil {
			log.Println("BoostManager: error inserting boost:", err)
		} else {
			log.Printf("BoostManager: inserted boost for nodeId=%X, ts=%d\n",
				boost.RawNodeIdBytes(), ts)
		}
		boostr.res <- struct{}{}
		q.Release()
	}
}

// read: protocol-byte, u16(msgLen), msg, u32(nBoosts)
// loop(nBoosts): read: u16(boosterLen), boosterBuf
func HandleBoostConn(conn net.Conn) {
	ln, f := utils.Lln("HandleBoostConn:"), utils.Lf("HandleBoostConn: ")

	defer conn.Close()
	res := make(chan struct{})
	defer close(res)

	var (
		t                  byte
		msgLen, boosterLen uint16
		nBoosts            uint32
		boosterBuf         []byte
		err                error
	)

	err0 := utils.ReadBin(conn, &t, &msgLen)
	msgBuf := make([]byte, msgLen)
	err1 := utils.ReadBin(conn, msgBuf, &nBoosts)
	if err = errors.Join(err0, err1); err != nil {
		ln("error reading request:", err)
	}

	f("t=%x, msgLen=%d, nBoosts=%d\n", t, msgLen, nBoosts)

	if t != 0x88 {
		f("wrong t byte:%x\n", t)
		return
	}

	msg := flatgen.GetRootAsMessageEvent(msgBuf, 0)
	cid := msg.ChatId(nil)
	boostMsgTimestamp := utils.MakeTimestamp()
	msgNonce := id_utils.RandU32()
	cid.MutateTimestamp(boostMsgTimestamp)
	cid.MutateU32(msgNonce)

	root := id_utils.MakeRawRoot(cid.Root(nil))
	man := getBoostMan(root)

	const msgStmt = `INSERT INTO db_one.messages (root, ts, nonce, msg)
                         VALUES (?, ?, ?, ?)`
	q := db.Scy.Query(msgStmt, root, boostMsgTimestamp, msgNonce, msgBuf)
	defer q.Release()
	if err = q.Exec(); err != nil {
		ln("error writing boost message:", err)
		return
	} else {
		f("inserted boostMsg in root=%X, ts=%d\n", root, boostMsgTimestamp)
	}

	for n := range nBoosts {
		lerr := utils.ReadBin(conn, &boosterLen)
		if lerr != nil {
			utils.WriteBin(conn, byte(0x89), n)
			ln("read boost len error:", err)
			return
		}

		if cap(boosterBuf) < int(boosterLen) {
			boosterBuf = make([]byte, boosterLen)
		}

		berr := utils.ReadBin(conn, boosterBuf[:boosterLen])
		if berr != nil {
			utils.WriteBin(conn, byte(0x89), n)
			ln("read boosterBuf error:", err)
			return
		}

		booster := flatgen.GetRootAsBooster(boosterBuf, 0)
		bmsgId := booster.MsgId(nil)
		bmsgId.MutateTimestamp(boostMsgTimestamp)
		bmsgId.MutateU32(msgNonce)
		man.boostCh <- &boostReq{boost: booster, res: res}
		<-res
	}
	ln("writing success byte")
	binary.Write(conn, binary.BigEndian, byte(0x88))
}
