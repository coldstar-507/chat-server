package handlers

import (
	"io"
	"log"
	"net/http"
	"time"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var DevConnsManager = devConnManager{
	add:    make(chan *conn),
	send:   make(chan *devMsg),
	conns:  make(map[string]*conn),
	ticker: time.NewTicker(time.Second * 5),
}

func (cm *devConnManager) remove(dev string) {
	cm.conns[dev].close <- struct{}{}
	close(cm.conns[dev].close)
	delete(cm.conns, dev)
}

func (cm *devConnManager) Run() {
	var (
		heartbeat = []byte{0x99}
		toRemove  = make([]string, 0, 24)
	)

	for {
		select {
		case <-cm.ticker.C:
			for k, c := range cm.conns {
				if _, err := c.w.Write(heartbeat); err != nil {
					toRemove = append(toRemove, k)
				} else {
					c.w.(http.Flusher).Flush()
				}
			}

			if len(toRemove) > 0 {
				log.Println("Removing from dev conns:", toRemove)
			}

			for _, x := range toRemove {
				cm.remove(x)
			}

			toRemove = toRemove[:0]

		case conn := <-cm.add:
			if cm.conns[conn.dev] != nil {
				log.Println("conn already exists, replacing it:", conn.dev)
				cm.remove(conn.dev)
			}
			cm.conns[conn.dev] = conn

		case msg := <-cm.send:
			if conn := cm.conns[msg.Dev]; conn != nil {
				if _, err := conn.w.Write(msg.Payload); err != nil {
					cm.remove(msg.Dev)
				} else {
					conn.w.(http.Flusher).Flush()
				}
			}
		}
	}
}

type devMsg struct {
	Dev     string
	Payload []byte
}

// these will be sse conns
type devConnManager struct {
	add    chan *conn
	send   chan *devMsg
	conns  map[string]*conn
	ticker *time.Ticker
}

type conn struct {
	dev   string
	w     io.Writer
	close chan struct{}
}

// passing conn by param doesn't feel safe but should be?
// unless a new push triggers an error -> disconnect
// while this is iterating, could possibly panic the whole program
func readNew(ref []byte, w io.Writer) error {
	prefix := tools.ExtractIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()
	for ok := it.Seek(ref); ok; ok = it.Next() {
		if _, err := w.Write(it.Value()); err != nil {
			return err
		}
		w.(http.Flusher).Flush()
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
	pid := pushReq.PushId(nil)
	pid.MutateTimestamp(utils.MakeTimestamp())

	dev := tools.FastBytesToString(pid.Prefix())
	DevConnsManager.send <- &devMsg{Dev: dev, Payload: pushReq.PayloadBytes()}

	// we save it for later too, dev likely not connected
	// we save it even though it's connected, perhaps redundant
	// but also simpler and perhaps more robust
	buf := tools.MediumPool.Get()
	defer tools.MediumPool.Put(buf)
	k := tools.WritePushIdValue(pid, buf)
	if err = db.LV.Put(buf[:k], pushReq.PayloadBytes(), nil); err != nil {
		log.Println("HandlePush error saving payload")
		w.WriteHeader(501)
	}
}

func HandleDevConn(w http.ResponseWriter, r *http.Request) {
	ref := tools.FastStringToBytes(r.PathValue("ref"))
	prefix := tools.ExtractNthIdPrefix(ref, 2)
	if err := readNew(ref, w); err != nil { // first read all new pushes
		log.Println("HandleDevConn error reading new, canceling connection:", err)
		w.WriteHeader(500)
		return
	}
	// then we connect, eliminating the race we previously had
	// between (go readNew) and the live connection
	conn := &conn{dev: tools.FastBytesToString(prefix), w: w, close: make(chan struct{})}
	DevConnsManager.add <- conn
	<-conn.close
}
