package handlers

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils2"
)

func StartDeviceServer() {
	listener, err := net.Listen("tcp", ":11004")
	utils2.Panic(err, "startDeviceServer error on net.Listen")
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
		} else {
			log.Println("Device Server: new chat connection:", conn.LocalAddr())
		}
		go HandleDevConn(conn)
	}
}

// type iddev = [id_utils.RAW_PUSH_ID_PREFIX_LEN]byte
// type pushId = [id_utils.RAW_PUSH_ID_LEN]byte

var DevConnsManager = devConnManager{
	add:    make(chan *iddevConn),
	send:   make(chan *push),
	conns:  make(map[utils2.Iddev_]*iddevConn),
	ticker: time.NewTicker(time.Second * 30),
}

func (cm *devConnManager) Run() {
	var (
		iddevArray = utils2.Iddev_{}
		iddevBuf   = bytes.NewBuffer(iddevArray[:0])
		heartbeat  = byte(0x99)
		toRemove   = make([]*utils2.Iddev_, 0, 24)
		pr         *flatgen.PushRequest
		sendReq    *push
	)

	for {
		select {
		case <-cm.ticker.C:
			for k, c := range cm.conns {
				if err := c.WriteBin(heartbeat); err != nil {
					toRemove = append(toRemove, &k)
				}
			}

			if len(toRemove) > 0 {
				log.Println("DCM: removing from dev conns:", toRemove)
			}

			for _, x := range toRemove {
				if c := cm.conns[*x]; c != nil {
					c.conn.Close()
					delete(cm.conns, *x)
				}
			}

			toRemove = toRemove[:0]

		case conn := <-cm.add:
			log.Printf("DCM: add request for conn=%x\n", *conn.iddev)
			if c := cm.conns[*conn.iddev]; c != nil {
				if conn.sess > c.sess {
					log.Printf("DCM: conn=%x newer conn, replacing it\n",
						*conn.iddev)
					c.conn.Close()
					delete(cm.conns, *c.iddev)
					cm.conns[*conn.iddev] = conn
					ReadPushes(conn.conn,
						(*conn.node)[:], conn.dev, conn.ref)
				} else {
					log.Printf("DCM: conn=%x isn't newer, destroying it\n",
						*conn.iddev)
					conn.conn.Close()
				}
			} else {
				log.Printf("DCM: adding conn=%x\n", *conn.iddev)
				cm.conns[*conn.iddev] = conn
				ReadPushes(conn.conn,
					(*conn.node)[:], conn.dev, conn.ref)
			}

		case sendReq = <-cm.send:
			pr = sendReq.pr
			stmt := `INSERT INTO db_one.pushes
                                 (node_id, dev, ts, nonce, type, payload)
                                 VALUES (?, ?, ?, ?, ?, ?)`
			ts := utils2.MakeTimestamp()
			q := db.Scy.Query(stmt, pr.RawNodeIdBytes(), pr.Dev(), ts,
				utils2.MakeNonce(), pr.Type(), pr.PayloadBytes())

			sendReq.ech <- q.Exec()
			q.Release()
			iddevBuf.Reset()
			err := utils2.WriteBin(iddevBuf,
				utils2.KIND_IDDEV, pr.RawNodeIdBytes(), pr.Dev())
			log.Printf("DCM: writing %x, %x, %x to iddevBuf\n",
				utils2.KIND_IDDEV, pr.RawNodeIdBytes(), pr.Dev())
			log.Printf("result: %x, %v\n", iddevArray, err)
			// iddev := iddevBuf.Bytes()
			// iddevBuf.Reset()

			log.Printf("DCM: push request for iddev=%X\n", iddevArray)
			if conn := cm.conns[iddevArray]; conn != nil {
				err := conn.WriteBin(pr.Type(), ts,
					uint16(len(pr.PayloadBytes())), pr.PayloadBytes())
				log.Printf("DCM: writing to conn at iddev=%x\n", iddevArray)
				if err != nil {
					conn.conn.Close()
					delete(cm.conns, *conn.iddev)
				}
			} else {
				log.Printf("DCM: no conn at iddev=%x\n", iddevArray)
				log.Println("DCM: those are the conns:")
				for k := range cm.conns {
					log.Printf("\t%x\n", k)
				}
			}

		}
	}
}

type push struct {
	pr  *flatgen.PushRequest
	ech chan error
}

type iddevConn struct {
	iddev     *utils2.Iddev_
	conn      net.Conn
	sess, ref int64
	node      *utils2.NodeId
	dev       uint32
	m         sync.Mutex
}

func (c *iddevConn) WriteBin(bin ...any) error {
	c.m.Lock()
	defer c.m.Unlock()
	return utils2.WriteBin(c.conn, bin...)
}

// these will be sse conns
type devConnManager struct {
	add   chan *iddevConn
	send  chan *push
	conns map[utils2.Iddev_]*iddevConn
	// conns  map[string]*conn
	ticker *time.Ticker
}

// passing conn by param doesn't feel safe but should be?
// unless a new push triggers an error -> disconnect
// while this is iterating, could possibly panic the whole program
// func readNew(ref []byte, w io.Writer) error {
// 	prefix := utils2.PushIdPrefix(ref)
// 	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
// 	defer it.Release()
// 	for ok := it.Seek(ref); ok; ok = it.Next() {
// 		if _, err := w.Write(it.Value()); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func ReadPushes(w io.Writer, nodeId []byte, dev uint32, ts int64) error {
	stmt := `SELECT ts, type, payload FROM db_one.pushes
                 WHERE node_id = ? AND dev = ? AND ts > ?`
	q := db.Scy.Query(stmt, nodeId, dev, ts)
	log.Println(q.String())
	defer q.Release()
	it := q.Iter()
	var (
		timestamp int64
		t         byte
		payload   []byte
	)
	for it.Scan(&timestamp, &t, &payload) {
		err := utils2.WriteBin(w, t, timestamp, uint16(len(payload)), payload)
		if err != nil {
			return err
		}
	}
	return it.Close()
}

func GetMsg(root []byte, ts int64, nonce uint32) ([]byte, error) {
	stmt := `SELECT msg FROM db_one.messages
                 WHERE root = ? AND ts = ? AND nonce = ?`
	q := db.Scy.Query(stmt, root, ts, nonce)
	defer q.Release()
	var msg []byte
	if err := q.Scan(&msg); err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}

func HandleGetMsg(w http.ResponseWriter, r *http.Request) {
	var (
		_root = utils2.Root{}
		root  = _root[:]
		ts    int64
		nonce uint32
		pad   byte
		err   error
	)

	if err = utils2.ReadBin(r.Body, &pad, root, &ts, &nonce); err != nil {
		log.Println("HandleGetMsg: error reading request:", err)
		w.WriteHeader(500)
	} else if msg, err := GetMsg(root, ts, nonce); err != nil {
		log.Println("HandleGetMsg: error finding msg:", err)
		w.WriteHeader(501)
	} else {
		w.Write(msg)
	}
}

func HandlePush(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("HandlePush error reading body:", err)
		w.WriteHeader(500)
		return
	}

	pushReq := flatgen.GetRootAsPushRequest(payload, 0)
	ech := make(chan error)
	defer close(ech)
	DevConnsManager.send <- &push{pr: pushReq, ech: ech}
	if err := <-ech; err != nil {
		log.Println("HandlePush error writing push to db:", err)
		w.WriteHeader(501)
	}
}

// conn comes in -> read new -> push -> connect
// possible to miss a push between read new and connect
// possible fix: read new from the connection manager
func HandleDevConn(conn net.Conn) {
	var (
		nodeId = utils2.NodeId{}
		dev    uint32
		ts     int64
		t      byte
		iddev  = utils2.Iddev_{}
	)
	if err := utils2.ReadBin(conn, &t, nodeId[:], &dev, &ts); err != nil {
		log.Println("HandleDevConn: error reading ref:", err)
		conn.Close()
		return
	}
	iddev[0] = t
	copy(iddev[1:], nodeId[:])
	binary.BigEndian.PutUint32(iddev[1+utils2.RAW_NODE_ID_LEN:], dev)
	// // copy(iddev[:], nodeId[:])
	// // binary.BigEndian.PutUint32(iddev[utils2.RAW_NODE_ID_LEN:], dev)
	// if err := ReadPushes(conn, nodeId[:], dev, ts); err != nil {
	// 	log.Println("HandleDevConn error reading new, canceling connection:", err)
	// 	conn.Close()
	// 	return
	// }

	DevConnsManager.add <- &iddevConn{
		iddev: &iddev,
		conn:  conn,
		sess:  utils2.MakeTimestamp(),
		ref:   ts,
		dev:   dev,
		node:  &nodeId,
	}
}

// func HandleDevConn(w http.ResponseWriter, r *http.Request) {
// 	ref, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		log.Println("HandleDevConn error reading body:", err)
// 		w.WriteHeader(500)
// 		return
// 	}

// 	var dev iddev
// 	pushId := flatgen.GetRootAsPushId(ref, 0)
// 	utils2.WritePushIdPrefixId(bytes.NewBuffer(dev[:0]), pushId)

// 	if err := readNew(ref, w); err != nil { // first read all new pushes
// 		log.Println("HandleDevConn error reading new, canceling connection:", err)
// 		w.WriteHeader(500)
// 		return
// 	}
// 	// then we connect, eliminating the race we previously had
// 	// between (go readNew) and the live connection
// 	// prefixHex := hex.EncodeToString(prefix)
// 	conn := &iddevConn{dev: &dev, w: w, close: make(chan struct{})}
// 	DevConnsManager.add <- conn
// 	<-conn.close
// }
