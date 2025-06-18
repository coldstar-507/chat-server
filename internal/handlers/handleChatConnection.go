package handlers

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils2"
)

const MAX_MSG_LEN int = 4096

var ccms *chatConnsManagers

var cm = &connMan{
	conns: make(map[utils2.Iddev_]*client),
	addCh: make(chan *client),
	delCh: make(chan *client),
}

type chatConnsManagers struct {
	nMan uint32
	mans []*Manager
}

func initChatConnsManagers(nMans uint32) {
	ccms = &chatConnsManagers{
		nMan: nMans,
		mans: make([]*Manager, nMans),
	}
	for i := range ccms.nMan {
		ccms.mans[i] = createMan(i)
		go ccms.mans[i].Run()
	}
}

func createMan(i uint32) *Manager {
	return &Manager{
		i:          i,
		conns:      make(map[utils2.Root]map[utils2.Iddev_]*client),
		delClient:  make(chan *client),
		sw:         make(chan *msgReq),
		connect:    make(chan *connRequest),
		disconnect: make(chan *connRequest),
		sem:        make(chan struct{}, 1000), // n routine lim
		wg:         sync.WaitGroup{},
	}
}

func (ncm *chatConnsManagers) GetMan(root *utils2.Root) *Manager {
	h := fnv.New32()
	h.Write(root[:])
	i := h.Sum32() % ncm.nMan
	return ncm.mans[i]
}
func StartChatServer() {
	listener, err := net.Listen("tcp", ":11002")
	utils2.Panic(err, "StartChatServer error on net.Listen")
	defer listener.Close()
	n, err := strconv.Atoi(os.Getenv("N_CHAT_MANAGERS"))
	utils2.Panic(err, "StartChatServer: undefined N_CHAT_MANAGERS")
	go cm.run()
	initChatConnsManagers(uint32(n))

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
		} else {
			log.Println("new chat connection:", conn.LocalAddr())
		}
		go HandleChatConn(conn)
	}
}

type client struct {
	rooms []utils2.Root
	iddev *utils2.Iddev_
	conn  *utils2.ClientConn
	sess  int64
	res   chan error
}

type msgReq struct {
	sender       *client
	msg          []byte
	root         *utils2.Root
	preSentMsgId *utils2.MsgId
	res          chan struct{}
}

type connRequest struct {
	client *client
	root   *utils2.Root
	res    chan struct{}
}

type rootRequest struct {
	sw      *msgReq
	newRoot *flatgen.Root
}

type connMan struct {
	conns        map[utils2.Iddev_]*client
	addCh, delCh chan *client
}

func (cm *connMan) run() {
	log.Println("cm: run()")
	for {
		select {
		case a := <-cm.addCh:
			log.Printf("cm: addReq:%x\n", *a.iddev)
			if co := cm.conns[*a.iddev]; co != nil {
				log.Printf("cm: co already there for %x\n", *a.iddev)
				if a.sess > co.sess {
					log.Printf("cm: newer sess for %x\n", *a.iddev)
					co.conn.C.Close()
					delete(cm.conns, *co.iddev)
					cm.conns[*a.iddev] = a
					a.res <- nil
				} else {
					log.Printf("cm: older sess for %x\n", *a.iddev)
					a.conn.C.Close()
					a.res <- errors.New("older session")
				}
			} else {
				log.Printf("cm: adding conn for %x\n", *a.iddev)
				cm.conns[*a.iddev] = a
				a.res <- nil
			}
		case r := <-cm.delCh:
			log.Printf("cm: delReq for %x\n", *r.iddev)
			if co := cm.conns[*r.iddev]; co != nil {
				log.Printf("cm: co is there for %x\n", *r.iddev)
				co.conn.C.Close()
				delete(cm.conns, *co.iddev)
			}
		}
	}
}

type Manager struct {
	i          uint32
	conns      map[utils2.Root]map[utils2.Iddev_]*client
	delClient  chan *client
	sw         chan *msgReq
	connect    chan *connRequest
	disconnect chan *connRequest
	sem        chan struct{}
	wg         sync.WaitGroup
}

func (m *Manager) Run() {
	// can be used to buffer some response data
	// var rp = bytes.NewBuffer(make([]byte, 0, 1024))
	log.Printf("man%d: running\n", m.i)
	for {
		log.Printf("man%d: print conns:\n", m.i)
		for root, room := range m.conns {
			log.Printf("man%d: root=%x\n", m.i, root[:])
			for iddev, client := range room {
				log.Printf("man%d: iddev=%x, client == nil -> %v\n",
					m.i, iddev[:], client == nil)
			}
		}

		select {
		// this is a request that comes from the client thread
		case del := <-m.delClient:
			log.Printf("man%d: del req for client=%X\n", m.i, del.iddev[:])
			log.Printf("man%d: client=%X has %d conns\n", m.i,
				del.iddev[:], len(del.rooms))
			for _, r := range del.rooms {
				if room := m.conns[r]; room != nil {
					c := room[*del.iddev]
					if c != nil && c.sess == del.sess {
						log.Printf("man%d: del client=%X from root=%x\n",
							m.i, c.iddev[:], r[:])
						delete(room, *del.iddev)
						if len(room) == 0 {
							log.Printf("man%d: root=%x empty:"+
								" removing", m.i, r[:])
							delete(m.conns, r)
						}
					}
				}
			}
			// the conn is potentially already closed
			// and it's not an issue, can call close on it multiple times
			log.Printf("man%d: freeing resources for client=%x\n", m.i, del.iddev[:])
			del.conn.C.Close()
			// tools.OneKbPool.Put(del.rbuf)
			// tools.FourKbPool.Put(del.cbuf)
			// close(del.res)

		case sw := <-m.sw:
			id := flatgen.GetRootAsMessageEvent(sw.msg, 0).ChatId(nil)
			log.Printf("man%d: sw from client=%X for root=%x\n",
				m.i, sw.sender.iddev[:], sw.root[:])
			// log.Printf("SUFFIX: %X\n", id.Suffix())
			if id.Suffix() == 0x00 {
				handleChat_(m, sw)
			} else {
				handleNonChat_(m, sw, id.Suffix() == 0x04)
			}
			sw.res <- struct{}{}

		case conn := <-m.connect:
			log.Printf("man%d: conn req from client=%x for root=%x\n",
				m.i, conn.client.iddev[:], conn.root[:])
			if room := m.conns[*conn.root]; room == nil {
				log.Printf("man%d: room doesn't exist, creating it\n", m.i)
				room = map[utils2.Iddev_]*client{
					*conn.client.iddev: conn.client,
				}
				m.conns[*conn.root] = room
			} else if client := room[*conn.client.iddev]; client == nil {
				log.Printf("man%d: adding the client to room\n", m.i)
				room[*conn.client.iddev] = conn.client
			} else if client.sess < conn.client.sess {
				log.Printf("man%d: client.sess: %d, newclient.sess: %d\n",
					m.i, client.sess, conn.client.sess)
				// replace
				log.Printf("man%d: older session already connected, replacing\n",
					m.i)
				room[*conn.client.iddev] = conn.client
				// triggers a delClient request, which
				// will free the resources
				client.conn.C.Close()
			} else if client.sess == conn.client.sess {
				log.Printf("man%d: already connected, ignoring\n", m.i)
			} else { // not a newer session
				log.Printf("man%d: already a newer session connected\n", m.i)
				// destroy that new client request
				// ressources haven't been allocated yet
				conn.client.conn.C.Close()
			}
			conn.res <- struct{}{}

		// a client might want to disconnect from a root
		case disc := <-m.disconnect:
			log.Printf("man%d: disc req from client=%x for root=%x\n",
				m.i, disc.client.iddev[:], disc.root[:])
			if room := m.conns[*disc.root]; room != nil {
				if client := room[*disc.client.iddev]; client != nil {
					if client.sess == disc.client.sess {
						log.Printf("man%d: proceeding to removal\n", m.i)
						delete(room, *disc.client.iddev)
					}
				}
			}
			disc.res <- struct{}{}
		}
	}
}

// should I just send snips here?
func handleNonChat_(m *Manager, sw *msgReq, isSnip bool) {
	log.Printf("man%d: handleing non-chat from client=%x in room=%x\n",
		m.i, sw.sender.iddev[:], sw.root[:])
	cid := flatgen.GetRootAsMessageEvent(sw.msg, 0).ChatId(nil)
	ts := utils2.MakeTimestamp()
	cid.MutateTimestamp(ts)

	if room := m.conns[*sw.root]; room != nil {
		for iddev, o := range room {
			if iddev != *sw.sender.iddev {
				m.wg.Add(1)
				m.sem <- struct{}{}
				go func() {
					defer m.wg.Done()
					defer func() { <-m.sem }()
					log.Printf("man%d: writing nonchat to"+
						" client=%x for room=%x\n",
						m.i, iddev[:], sw.root[:])
					o.conn.WriteBin(CHAT_EVENT, uint16(len(sw.msg)), sw.msg)
				}()
			}

		}
	}

	q := fmt.Sprintf(`INSERT INTO db_one.%s (root, ts, nonce, msg)
              VALUES (?, ?, ?, ?)`, If(isSnip, "snips", "messages"))
	db.Scy.Query(q, sw.root[:], ts, cid.U32(), sw.msg).Exec()

	m.wg.Wait()
}

// we send MESSAGE_SENT | LEN | OLD_ID | NEW_ID to sender
// we send CHAT_EVENT | LEN | MSG to conns (msg has the mutated id)
// we send MESSAGE_SAVE_ERROR | LEN | NEW_ID to sender if can't save
func handleChat_(m *Manager, sw *msgReq) {
	log.Printf("man%d: handleing chat from client=%x in room=%x\n",
		m.i, sw.sender.iddev[:], sw.root[:])

	cht := flatgen.GetRootAsMessageEvent(sw.msg, 0)
	chatId := cht.ChatId(nil)

	ts := utils2.MakeTimestamp()
	chatId.MutateTimestamp(ts)
	chatId.MutateSuffix(0x03)

	// sem := make(chan struct{}, 1000) // n routine lim
	// var wg sync.WaitGroup
	if room := m.conns[*sw.root]; room != nil {
		for iddev, o := range room {
			if iddev != *sw.sender.iddev {
				m.wg.Add(1)
				m.sem <- struct{}{}
				go func() {
					defer m.wg.Done()
					defer func() { <-m.sem }()
					o.conn.WriteBin(CHAT_EVENT, uint16(len(sw.msg)), sw.msg)
					log.Printf("man%d: writing chat to client=%x"+
						" in room=%x\n", m.i, iddev[:], sw.root[:])
					// log.Println("o == nil", o == nil)
				}()
			}
		}
	}

	// write sent to sender
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		sw.sender.conn.Locked(func(w io.Writer) {
			l := uint16(2 * utils2.RAW_MSG_ID_LEN)
			utils2.WriteBin(w, MESSAGE_SENT, l, sw.preSentMsgId[:])
			utils2.WriteMsgId(w, chatId)
		})
	}()

	stmt := "INSERT INTO db_one.messages (root, ts, nonce, msg) VALUES (?, ?, ?, ?)"
	if err := db.Scy.Query(stmt, sw.root[:],
		ts, chatId.U32(), sw.msg).Exec(); err != nil {
		log.Printf("man%d: handleChat_: error saving message %v:\n", m.i, err)
		sw.sender.conn.Locked(func(w io.Writer) {
			utils2.WriteBin(w, MESSAGE_SAVE_ERROR,
				uint16(utils2.RAW_MSG_ID_LEN))
			utils2.WriteMsgId(w, chatId)
		})
	}

	m.wg.Wait()
}

func HandleChatConn(conn net.Conn) {
	iddev := utils2.Iddev_{}
	if _, err := io.ReadFull(conn, iddev[:]); err != nil {
		log.Println("HandleChatConn: error reading iddev:", err)
		conn.Close()
		return
	}
	c := &client{
		iddev: &iddev,
		conn:  utils2.NewLockedConn(conn), // .ClientConn{C: conn, m: sync.Mutex{}},
		sess:  utils2.MakeTimestamp(),
		// cbuf:  tools.FourKbPool.Get(),
		// rbuf:  tools.OneKbPool.Get(),
		rooms: make([]utils2.Root, 0, 5),
		res:   make(chan error),
	}
	cm.addCh <- c
	if err := <-c.res; err != nil {
		log.Printf("HandleChatConn err for %x: %v\n", iddev, err)
		close(c.res)
		return
	}
	// no use after this, so close it right away
	close(c.res)
	go c.readFromClientSync_()

}

func (c *client) readFromClientSync_() {
	var (
		reqType         byte
		reqLen          uint16
		isBefore        bool
		limit           uint16
		isSnips         bool
		ts              int64
		msgBuf          = tools.FourKbPool.Get()
		root            utils2.Root
		rootBuf         = bytes.NewBuffer(root[:0])
		preSentMsgId    utils2.MsgId
		preSentMsgIdBuf = bytes.NewBuffer(preSentMsgId[:0])
		err             error
	)

	defer tools.FourKbPool.Put(msgBuf)
	defer log.Printf("client=%x: thread was closed\n", c.iddev[:])

	go func() {
		ticker := time.NewTicker(time.Second * 20)
		defer log.Printf("killed heartbeater for %x, sess=%d\n", *c.iddev, c.sess)
		defer ticker.Stop()
		var heartbeatErr error
		for {
			<-ticker.C
			heartbeatErr = c.conn.WriteBin(byte(0x99))
			if heartbeatErr != nil {
				log.Printf("client=%x, sess=%d heartbeat err: %v\n",
					*c.iddev, c.sess, heartbeatErr)
				c.conn.C.Close()
				return
			}
			log.Printf("client=%x, sess=%d, heartbeat", *c.iddev, c.sess)
		}
	}()

	for {
		log.Printf("client=%x: reading from client\n", c.iddev[:])
		log.Printf("client=%x: conns:\n", c.iddev[:])
		for _, x := range c.rooms {
			log.Printf("\t%x\n", x[:])
		}

		// sw req -> close() -> close(sent) -> sw sent
		// this sequence could theoratically crash the server
		// so manager should be responsible for freeing client resources

		err = utils2.ReadBin(c.conn.C, &reqType, &reqLen)
		if err != nil {
			log.Printf("client=%x:, sess=%d, readBin error=%v, closing client\n",
				*c.iddev, c.sess, err)
			for _, root := range c.rooms {
				ccms.GetMan(&root).delClient <- c
				// ConnManager.delClient <- c
			}
			cm.delCh <- c
			return
		}

		switch reqType {
		case CHAT_CONN_REQ:
			log.Printf("client=%x: [BEGIN] chat conn request\n", c.iddev[:])
			if _, err = c.conn.C.Read(root[:]); err != nil {
				log.Printf("client=%x error reading root: %v\n",
					c.iddev[:], err)
				continue
			}
			if utils2.Contains(root, c.rooms) {
				log.Printf("client=%x: already connected to root=%x\n",
					c.iddev[:], root[:])
				continue
			}
			res := make(chan struct{})
			cr := &connRequest{res: res, client: c, root: &root}
			ccms.GetMan(&root).connect <- cr
			// ConnManager.connect <- &connRequest{client: c, root: &root}
			<-res
			close(res)
			c.rooms, _ = utils2.AddToSet(root, c.rooms)
			c.conn.WriteBin(byte(0x16), uint16(len(root)), root[:])
			log.Printf("client=%x: [DONE] chat conn request\n", c.iddev[:])

		case CHAT_DISC_REQ:
			log.Printf("client=%x: [BEGIN] chat disc request\n", c.iddev[:])
			if _, err = c.conn.C.Read(root[:]); err != nil {
				log.Printf("client=%x error reading root: %v\n",
					c.iddev[:], err)
				continue
			}
			if !utils2.Contains(root, c.rooms) {
				log.Printf("client=%x: already disc to root=%x\n",
					c.iddev[:], root[:])
				continue
			}
			res := make(chan struct{})
			cr := &connRequest{res: res, client: c, root: &root}
			ccms.GetMan(&root).disconnect <- cr
			// ConnManager.disconnect <- &connRequest{client: c, root: &root}
			<-res
			close(res)
			c.rooms, _ = utils2.Remove(root, c.rooms)
			c.conn.WriteBin(byte(0x17), uint16(len(root)), root[:])
			log.Printf("client=%x: [DONE] chat disc request\n", c.iddev[:])

		case SCROLL_REQUEST:
			log.Printf("client=%x: [BEGIN] scroll request\n", c.iddev[:])
			err = utils2.ReadBin(c.conn.C, &isBefore, root[:], &ts,
				&limit, &isSnips)
			if err != nil {
				log.Printf("client=%x: error reading rest of request\n",
					c.iddev[:])
				continue
			}
			log.Printf("client=%x: before=%v, root=%x, ts=%d, limit=%d\n",
				c.iddev[:], isBefore, root[:], ts, limit)

			HandleChatScrollSync(c.conn, isBefore, root[:], ts,
				limit, isSnips)
			log.Printf("client=%x: [DONE] scroll request\n", c.iddev[:])

		case BOOSTS_REQ:
			log.Printf("client=%x: [BEGIN] boost req\n", c.iddev[:])
			nodeId := msgBuf[:utils2.RAW_NODE_ID_LEN]
			err = utils2.ReadBin(c.conn.C, nodeId, &ts, &limit)
			if err != nil {
				log.Printf("client=%x: error reading request: %v\n",
					c.iddev[:], err)
				continue
			}
			HandleBoostScroll(c.conn, nodeId, ts, limit)
			log.Printf("client=%x: [DONE] boost req\n", c.iddev[:])

		case CHAT_EVENT:
			mbuf := msgBuf[:reqLen]
			log.Printf("client=%x: [BEGIN] chat event\n", c.iddev[:])
			if _, err = c.conn.C.Read(mbuf); err != nil {
				log.Printf("client=%x: error reading msg: %v\n",
					c.iddev[:], err)
				continue
			}
			cid := flatgen.GetRootAsMessageEvent(mbuf, 0).ChatId(nil)
			utils2.WriteMsgId(preSentMsgIdBuf, cid)
			preSentMsgIdBuf.Reset()
			if rt := cid.Root(nil); !rt.Confirmed() {
				newRoot, _, err := GetOrPushRoot(rt.UnPack())
				if err != nil {
					log.Printf("client=%x: error getting root: %v\n",
						c.iddev[:], err)
					c.conn.WriteBin(ROOT_ERROR, uint16(0))
					continue
				} else if newRoot.ChatPlace != rt.ChatPlace() {
					log.Printf("client=%x: newroot has otherplace\n",
						c.iddev[:])
					c.conn.Locked(func(w io.Writer) {
						l := utils2.RAW_ROOT_ID_LEN +
							utils2.RAW_MSG_ID_LEN
						utils2.WriteBin(w,
							NEW_ROOT_PLACE, uint16(l))
						utils2.WriteRootT(w, newRoot)
						utils2.WriteMsgId(w, cid)
					})
					continue
				}
				rt.MutateTimestamp(newRoot.Timestamp)
				rt.MutateConfirmed(true)

				// connect to that root
				utils2.WriteRoot(rootBuf, rt)
				res := make(chan struct{})
				cr := &connRequest{res: res, client: c, root: &root}
				ccms.GetMan(&root).connect <- cr
				// ConnManager.connect <- &connRequest{client: c,
				// 	root: &root}
				<-res
				close(res)
				c.rooms, _ = utils2.AddToSet(root, c.rooms)
				c.conn.WriteBin(byte(0x16), uint16(len(root)), root[:])

			} else {
				utils2.WriteRoot(rootBuf, cid.Root(nil))
			}

			rootBuf.Reset()
			log.Printf("client=%x: root=%X\n", c.iddev[:], root[:])
			res := make(chan struct{})
			ccms.GetMan(&root).sw <- &msgReq{res: res, sender: c, msg: mbuf,
				root: &root, preSentMsgId: &preSentMsgId}
			// ConnManager.sw <- &msgReq{sender: c, msg: mbuf,
			// 	root: &root, preSentMsgId: &preSentMsgId}
			<-res
			close(res)
			log.Printf("client=%x: [DONE] chat event\n", c.iddev[:])

		case NOTIFICATIONS:
			ntfbuf := msgBuf[:reqLen]
			if _, err := c.conn.C.Read(ntfbuf); err != nil {
				log.Printf("client=%x: error reading ntf: %v",
					c.iddev[:], err)
				continue
			}
			log.Printf("client=%x: [BEGIN] notification\n", c.iddev[:])
			ntfs := flatgen.GetRootAsNotifications(ntfbuf, 0)
			SendNotifications(ntfs)
			log.Printf("client=%x: [DONE] notification\n", c.iddev[:])
		}
	}
}
