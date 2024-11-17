package handlers

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func StartDeviceServer() {
	listener, err := net.Listen("tcp", ":11004")
	utils.Panic(err, "startDeviceServer error on net.Listen")
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

type iddev = [utils.RAW_PUSH_ID_PREFIX_LEN]byte
type pushId = [utils.RAW_PUSH_ID_LEN]byte

var DevConnsManager = devConnManager{
	add:    make(chan *iddevConn),
	send:   make(chan *push),
	conns:  make(map[iddev]*iddevConn),
	ticker: time.NewTicker(time.Second * 30),
}

func (cm *devConnManager) Run() {
	var (
		pushIdArray   = [utils.RAW_PUSH_ID_LEN]byte{}
		prefixIdArray = [utils.RAW_PUSH_ID_PREFIX_LEN]byte{}
		heartbeat     = []byte{0x99}
		toRemove      = make([]*iddev, 0, 24)
		pushIdBuffer  = bytes.NewBuffer(pushIdArray[:0])
		prefixBuffer  = bytes.NewBuffer(prefixIdArray[:0])
		pushReq       *flatgen.PushRequest
		pushId        *flatgen.PushId
		sendReq       *push
		ts            int64
	)

	for {
		select {
		case <-cm.ticker.C:
			for k, c := range cm.conns {
				if _, err := c.conn.Write(heartbeat); err != nil {
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
			log.Printf("DCM: add request for conn=%x\n", *conn.dev)
			if c := cm.conns[*conn.dev]; c != nil {
				if conn.sess > c.sess {
					log.Printf("DCM: conn=%x newer conn, replacing it\n",
						*conn.dev)
					c.conn.Close()
					delete(cm.conns, *c.dev)
					cm.conns[*conn.dev] = conn
				} else {
					log.Printf("DCM: conn=%x isn't newer, destroying it\n",
						*conn.dev)
					conn.conn.Close()
				}
			} else {
				log.Printf("DCM: adding conn=%x\n", *conn.dev)
				cm.conns[*conn.dev] = conn
			}

		case sendReq = <-cm.send:
			ts = utils.MakeTimestamp()
			pushReq = sendReq.pr
			pushId = pushReq.PushId(nil)
			pushId.MutateTimestamp(ts)
			utils.WritePushId(pushIdBuffer, pushId)
			sendReq.ech <- db.LV.Put(pushIdBuffer.Bytes(),
				pushReq.PayloadBytes(), nil)

			utils.WritePushIdPrefixId(prefixBuffer, pushId)

			log.Printf("DCM: push request id=%x for iddev=%x\n",
				pushIdBuffer, prefixBuffer)

			if conn := cm.conns[prefixIdArray]; conn != nil {
				log.Printf("DCM: writing to conn at iddev=%x\n", prefixIdArray)
				_, err := conn.conn.Write(pushReq.PayloadBytes())
				if err != nil {
					conn.conn.Close()
					delete(cm.conns, *conn.dev)
				}
			} else {
				log.Printf("DCM: no conn at iddev=%x\n", prefixIdArray)
				log.Println("DCM: those are the conns:")
				for k := range cm.conns {
					log.Printf("\t%x\n", k)
				}

			}

			pushIdBuffer.Reset()
			prefixBuffer.Reset()
		}

	}
}

type push struct {
	pr  *flatgen.PushRequest
	ech chan error
}

type iddevConn struct {
	dev  *iddev
	conn net.Conn
	sess int64
	// close chan struct{}
}

// these will be sse conns
type devConnManager struct {
	add   chan *iddevConn
	send  chan *push
	conns map[iddev]*iddevConn
	// conns  map[string]*conn
	ticker *time.Ticker
}

// passing conn by param doesn't feel safe but should be?
// unless a new push triggers an error -> disconnect
// while this is iterating, could possibly panic the whole program
func readNew(ref []byte, w io.Writer) error {
	prefix := utils.PushIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()
	for ok := it.Seek(ref); ok; ok = it.Next() {
		if _, err := w.Write(it.Value()); err != nil {
			return err
		}
	}
	return nil
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

func HandleDevConn(conn net.Conn) {
	var (
		pushId = pushId{}
		iddev  = iddev{}
	)

	if _, err := conn.Read(pushId[:]); err != nil {
		log.Println("HandleDevConn2: error reading ref:", err)
		conn.Close()
		return
	}

	copy(iddev[:], pushId[:utils.RAW_PUSH_ID_PREFIX_LEN])
	if err := readNew(iddev[:], conn); err != nil {
		log.Println("HandleDevConn2 error reading new, canceling connection:", err)
		conn.Close()
		return
	}

	DevConnsManager.add <- &iddevConn{dev: &iddev, conn: conn, sess: utils.MakeTimestamp()}

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
// 	utils.WritePushIdPrefixId(bytes.NewBuffer(dev[:0]), pushId)

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
