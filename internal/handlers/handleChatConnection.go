package handlers

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
)

const MAX_MSG_LEN int = 4096

// var cm = chatManager{
// 	chats: make(map[string]*room),
// 	m:     sync.Mutex{},
// }

// type chatManager struct {
// 	m     sync.Mutex
// 	chats map[string]*room
// }

type Cm2 struct {
	rooms   map[string]*room2
	add, rm chan *client2
}

var ChatConnsManager = &Cm2{
	rooms: make(map[string]*room2),
	add:   make(chan *client2),
	rm:    make(chan *client2),
}

type client2 struct {
	rdy        chan struct{}
	room       *room2
	sess       int64
	dev, root  string
	conn       net.Conn
	cbuf, rbuf []byte
}

type room2 struct {
	root string
	rwm  sync.RWMutex
	// key is a deviceId
	clients map[string]*client2
}

type Req struct {
	t byte
	l uint16
	b []byte
}

func readReq(c net.Conn, r *Req, cbuf []byte) error {
	var err error
	if _, err = io.ReadFull(c, cbuf[:1]); err != nil {
		return fmt.Errorf("message type read error: %v", err)
	}
	r.t = cbuf[0]
	if _, err = io.ReadFull(c, cbuf[1:3]); err != nil {
		return fmt.Errorf("message len read error: %v", err)
	}
	r.l = binary.BigEndian.Uint16(cbuf[1:3])
	if 3+r.l > uint16(MAX_MSG_LEN) {
		panic("message too big for buffer, should not be allowed by client")
	}
	if _, err = io.ReadFull(c, cbuf[3:3+r.l]); err != nil {
		return fmt.Errorf("message body read error: %v", err)
	}
	r.b = cbuf[:3+r.l]
	return nil
}

func handleNonChat(cid *flatgen.MessageId, c *client2, req *Req) {
	cid.MutateTimestamp(utils.MakeTimestamp())
	c.room.rwm.RLock()
	for dev, o := range c.room.clients {
		if dev != c.dev {
			log.Println("writing non-chat to dev:", dev)
			o.conn.Write(req.b)
		}
	}
	c.room.rwm.RUnlock()
	// can just use the response buffer to write the id for LV.Put
	idlen := tools.WriteMsgIdValue(cid, c.rbuf)
	db.LV.Put(c.rbuf[:idlen], req.b, nil)
}

func handleChat(cid *flatgen.MessageId, c *client2, req *Req) {
	idLen := tools.WriteMsgIdValue(cid, c.rbuf[3:])
	cid.MutateTimestamp(utils.MakeTimestamp())
	tools.WriteMsgIdValue(cid, c.rbuf[3+idLen:])
	fullLen := 3 + idLen*2
	freshId := c.rbuf[3+idLen : fullLen]
	c.rbuf[0] = MESSAGE_SENT
	binary.BigEndian.PutUint16(c.rbuf[1:], uint16(idLen*2))
	c.room.rwm.RLock()
	for dev, o := range c.room.clients {
		if dev != c.dev {
			log.Println("writing chat to dev:", dev)
			o.conn.Write(req.b)
		} else {
			log.Println("writing sent to dev:", dev)
			c.conn.Write(c.rbuf[:fullLen])
		}
	}
	c.room.rwm.RUnlock()
	if err := db.LV.Put(freshId, req.b, nil); err != nil {
		log.Println("LDB.Put(MessageEvent) error:", err)
		c.rbuf[0] = MESSAGE_SAVE_ERROR
		c.conn.Write(c.rbuf[:fullLen])
	}
}

func parseConnReq(conn net.Conn, cbuf []byte) (string, string, error) {
	var l uint16
	var err error
	if _, err = io.ReadFull(conn, cbuf[:2]); err != nil {
		return "", "", err
	}
	l = binary.BigEndian.Uint16(cbuf[:2])
	if _, err = io.ReadFull(conn, cbuf[2:2+l]); err != nil {
		return "", "", err
	}
	vals := strings.Split(string(cbuf[2:2+l]), " ")
	var root, dev = vals[0], vals[1]
	log.Printf("new chat connection, root=%s, dev=%s\n", root, dev)
	return root, dev, nil
}

func (cm *Cm2) Run() {
	for {
		select {
		case c := <-cm.add:
			log.Printf("[ADD] root=%s, dev=%s, sess=%d\n", c.root, c.dev, c.sess)
			if r := cm.rooms[c.root]; r == nil {
				log.Println("Room doesn't exists, creating it!")
				clients := make(map[string]*client2)
				clients[c.dev] = c
				room := &room2{root: c.root, rwm: sync.RWMutex{}, clients: clients}
				cm.rooms[c.root] = room
				c.room = room
			} else {
				log.Printf("Room exists, adding client")
				c.room = r
				r.rwm.Lock()
				if cl := r.clients[c.dev]; cl != nil && cl.sess != c.sess {
					log.Printf("Client already connected, replacing it")
					// overriding session with a new one
					delete(r.clients, cl.dev)
					cl.conn.Close()
					close(cl.rdy)
					tools.FourKbPool.Put(cl.cbuf)
					tools.OneKbPool.Put(cl.rbuf)
				}
				r.clients[c.dev] = c
				r.rwm.Unlock()
			}

			log.Printf("Client dev=%s added to room=%s\n", c.dev, c.root)
			c.rdy <- struct{}{}

		case c := <-cm.rm:
			log.Printf("[RM] root=%s, dev=%s, sess=%d\n", c.root, c.dev, c.sess)
			if r := cm.rooms[c.root]; r != nil {
				if cl := r.clients[c.dev]; cl != nil && cl.sess == c.sess {
					log.Println("Client valid for removale, removing it")
					r.rwm.Lock()
					delete(r.clients, cl.dev)
					r.rwm.Unlock()
					close(cl.rdy)
					tools.FourKbPool.Put(cl.cbuf)
					tools.OneKbPool.Put(cl.rbuf)

					if len(r.clients) == 0 {
						log.Printf("Room=%s is empty, removing it\n", c.root)
						delete(cm.rooms, c.root)
					}
					
				} else if cl != nil && cl.sess != c.sess {
					log.Println("Session doesn't match, not removing it")
				}
			}
		}
	}
}

func (c *client2) readFromClient() {
	var (
		me         *flatgen.MessageEvent2
		cid        *flatgen.MessageId
		req        = &Req{}
		bres, ares = tools.TinyPool.Get(), tools.TinyPool.Get()
		bref, aref = tools.MediumPool.Get(), tools.MediumPool.Get()
	)

	for {
		log.Println("reading from client")
		if err := readReq(c.conn, req, c.cbuf); err != nil {
			tools.TinyPool.Put(ares)
			tools.TinyPool.Put(bres)
			tools.MediumPool.Put(aref)
			tools.MediumPool.Put(bref)
			ChatConnsManager.rm <- c
			return
		}

		log.Println("req.t:", req.t, "len(req.b):", len(req.b))

		switch req.t {
		case SCROLL_REQUEST:
			// both before and after scrolls can be active at the same time
			// but there is only one of a kind at a time (handled from client side)
			// but it can be concurrent with read from client
			// -- alternative would be to by sync with read from client
			// -- would probably be good as well
			log.Println("received scroll request")
			before := req.b[3] == 0x01
			if before {
				copy(bref, req.b[4:])
				log.Println("scrolling before:", string(req.b[4:]))
				go handleChatScrollBefore(bref, bres, c.conn)
			} else {
				copy(aref, req.b[4:])
				log.Println("scrolling after:", string(req.b[4:]))
				go handleChatScrollAfter(aref, ares, c.conn)
			}
		case CHAT_EVENT:
			log.Println("received chat event")
			me = flatgen.GetRootAsMessageEvent2(req.b, 3)
			cid = flatgen.GetRootAsMessageId(me.ChatIdBytes(), 0)
			if me.Type() == 0x00 { // it's a chat
				handleChat(cid, c, req)
			} else {
				handleNonChat(cid, c, req)
			}
		case NOTIFICATIONS:
			log.Println("received notification")
			ntfs := flatgen.GetRootAsNotifications(req.b, 3)
			SendNotifications(ntfs)
		}
	}
}

func HandleChatConn(conn net.Conn) {
	cbuf := tools.FourKbPool.Get()
	root, dev, err := parseConnReq(conn, cbuf)
	if err != nil {
		tools.FourKbPool.Put(cbuf)
		conn.Close()
		return
	}
	sess := utils.MakeTimestamp()
	rbuf := tools.OneKbPool.Get()
	rdy := make(chan struct{})
	c := &client2{rdy: rdy, dev: dev, root: root, sess: sess, conn: conn, cbuf: cbuf, rbuf: rbuf}
	ChatConnsManager.add <- c
	<-c.rdy
	go c.readFromClient()
}

////////////////////////////////////////////////////////////////////////////////////

// type room struct {
// 	root string
// 	rwm  sync.RWMutex
// 	// key is a deviceId
// 	clients map[string]*client
// }

// type client struct {
// 	sessId int64
// 	chat   *room
// 	dev    string
// 	conn   net.Conn
// 	cbuf   []byte
// 	resbuf []byte
// }

// // can save [0x00, len(me), me] to database
// // makes it very easy to stream it back in chat scrolls
// // by doing this, we remove the need of having a lock on the client connection
// // making it truly the *simplest* implementation
// // -- not that it would be that hard to lock the writer to write
// // -- the header, the len and then the me

// // -- but maybe w.Write flushes on write, and having no lock
// // -- and no flush on write will definetly be more performant
// func (c *client) readFromClient4() {
// 	var (
// 		me         *flatgen.MessageEvent2
// 		cid        *flatgen.MessageId
// 		req        = &Req{}
// 		bres, ares = tools.TinyPool.Get(), tools.TinyPool.Get()
// 		bref, aref = tools.MediumPool.Get(), tools.MediumPool.Get()
// 	)

// 	putback := func() {
// 		tools.FourKbPool.Put(&c.cbuf)
// 		tools.OneKbPool.Put(&c.resbuf)
// 		tools.MediumPool.Put(&bref)
// 		tools.MediumPool.Put(&aref)
// 		tools.TinyPool.Put(&bres)
// 		tools.TinyPool.Put(&ares)
// 	}

// 	for {
// 		log.Println("reading from client")
// 		if err := readReq(c.conn, req, c.cbuf); err != nil {
// 			rt := c.chat.root
// 			if cc := cm.chats[rt].clients[c.dev]; cc != nil && cc.sessId == c.sessId {
// 				cc.conn.Close() // can ignore error if already closed
// 				tools.FourKbPool.Put(&cc.cbuf)
// 				tools.OneKbPool.Put(&cc.resbuf)
// 				delete(cm.chats[c.chat.root].clients, c.dev)
// 			}
// 			return
// 		}

// 		log.Println("req.t:", req.t, "len(req.b):", len(req.b))

// 		switch req.t {
// 		case SCROLL_REQUEST:
// 			// both before and after scrolls can be active at the same time
// 			// but there is only one of a kind at a time (handled from client side)
// 			// but it can be concurrent with read from client
// 			// -- alternative would be to by sync with read from client
// 			// -- would probably be good as well
// 			log.Println("received scroll request")
// 			before := req.b[3] == 0x01
// 			if before {
// 				copy(bref, req.b[4:])
// 				log.Println("scrolling before:", string(req.b[4:]))
// 				go handleChatScrollBefore(bref, bres, c.conn)
// 			} else {
// 				copy(aref, req.b[4:])
// 				log.Println("scrolling after:", string(req.b[4:]))
// 				go handleChatScrollAfter(aref, ares, c.conn)
// 			}
// 		case CHAT_EVENT:
// 			log.Println("received chat event")
// 			me = flatgen.GetRootAsMessageEvent2(req.b, 3)
// 			cid = flatgen.GetRootAsMessageId(me.ChatIdBytes(), 0)
// 			if me.Type() == 0x00 { // it's a chat
// 				handleChat(cid, c, req)
// 			} else {
// 				handleNonChat(cid, c, req)
// 			}
// 		case NOTIFICATIONS:
// 			log.Println("received notification")
// 			ntfs := flatgen.GetRootAsNotifications(req.b, 3)
// 			SendNotifications(ntfs)
// 		}
// 	}
// }

// func (c *client) disconnect(err error) {
// 	c.chat.rwm.Lock()
// 	log.Printf("disconnecting client id=%s, error?=%v\n", c.dev, err)
// 	delete(c.chat.clients, c.dev)
// 	if len(c.chat.clients) == 0 {
// 		cm.m.Lock()
// 		log.Printf("removing chat room=%s from chat manager\n", c.chat.root)
// 		delete(cm.chats, c.chat.root)
// 		cm.m.Unlock()
// 	}
// 	c.chat.rwm.Unlock()
// 	c.conn.Close()
// }

// func HandleChatConn3(conn net.Conn) {
// 	cbuf := tools.FourKbPool.Get()
// 	root, dev, err := parseConnReq_(conn, cbuf)
// 	if err != nil {
// 		conn.Close()
// 		return
// 	}
// 	var c *client
// 	cm.m.Lock()
// 	if cm.chats[root] == nil { // need to create the chat room
// 		log.Println("creating chat room for root:", root)
// 		chat := &room{root: root, rwm: sync.RWMutex{}, clients: make(map[string]*client)}
// 		rbuf := tools.OneKbPool.Get()
// 		c = &client{chat: chat, dev: dev, conn: conn, cbuf: cbuf, resbuf: rbuf}
// 		chat.clients[dev] = c
// 		cm.chats[root] = chat
// 	} else { // need to add client to the chat
// 		log.Printf("adding client dev=%s, to room root=%s\n", dev, root)
// 		rbuf := tools.OneKbPool.Get()
// 		c = &client{chat: cm.chats[root], dev: dev, conn: conn, cbuf: cbuf, resbuf: rbuf}
// 		cm.chats[root].rwm.Lock()
// 		if cc := cm.chats[root].clients[dev]; cc != nil {
// 			cc.conn.Close()
// 			tools.FourKbPool.Put(&cc.cbuf)
// 			tools.OneKbPool.Put(&cc.resbuf)
// 			delete(cm.chats[root].clients, dev)
// 		}
// 		cm.chats[root].clients[dev] = c
// 		cm.chats[root].rwm.Unlock()
// 	}
// 	cm.m.Unlock()
// 	go c.readFromClient4()
// }

// type chatManager2 struct {
// 	addClient chan *client2
// 	rmClient  chan *client2
// 	broadcast chan *chatMessage
// 	rooms     map[string](map[string]*client2)
// 	// rooms     map[string]*room2
// }

// var cm2 = chatManager2{
// 	addClient: make(chan *client2),
// 	rmClient:  make(chan *client2),
// 	broadcast: make(chan *chatMessage),
// 	rooms:     make(map[string](map[string]*client2)),
// }

// type chatMessage struct {
// 	dev  string
// 	room string
// 	buf  []byte
// }

// func (cm *chatManager2) RunManager() {
// 	for {
// 		select {
// 		case c := <-cm.addClient:
// 			if cm.rooms[c.room][c.dev] == nil {
// 				cm.rooms[c.room][c.dev] = c
// 			}
// 		case c := <-cm.rmClient:
// 			// the reader and the writer can return an error
// 			// and initiate the client shutdown, thus, this can be nil
// 			if cm.rooms[c.room][c.dev] == nil {
// 				break
// 			}
// 			c.mch <- nil // this is how we tell the listener it's over
// 			delete(cm.rooms[c.room], c.dev)
// 			if len(cm.rooms[c.room]) == 0 {
// 				delete(cm.rooms, c.room)
// 			}
// 		case br := <-cm.broadcast:
// 			for k, v := range cm.rooms[br.room] {
// 				// broadcast to everyone except the sender
// 				if k != br.dev {
// 					v.mch <- br.buf
// 				}
// 			}
// 		}
// 	}
// }

// type client2 struct {
// 	room string
// 	dev  string
// 	mch  chan []byte
// 	conn net.Conn
// }

// func (c *client2) listenToMessages() {
// 	for {
// 		if x := <-c.mch; x == nil {
// 			c.conn.Close()
// 			close(c.mch)
// 			return
// 		} else if _, err := c.conn.Write(x); err != nil {
// 			cm2.rmClient <- c
// 		}
// 	}
// }

// func HandleChatConn2(conn net.Conn) {
// 	cbuf := make([]byte, 4096)
// 	ccr := parseConnReq(conn, cbuf)
// 	if ccr == nil {
// 		conn.Close()
// 		return
// 	}
// 	root, dev := string(ccr.Root()), string(ccr.DeviceId())
// 	c := &client2{room: root, dev: dev, mch: make(chan []byte, 10), conn: conn}
// 	cm2.addClient <- c
// 	go c.listenToMessages()
// 	go c.readFromClient2(cbuf)
// }

// func (c *client2) readFromClient2(cbuf []byte) {

// 	// const MAX_MSG_LEN int = 4096
// 	// cbuf := make([]byte, MAX_MSG_LEN)
// 	resbuf := make([]byte, 512)
// 	var idLen int
// 	var err error
// 	var req = &Req{}
// 	for {
// 		if err = readReq(c.conn, req, cbuf); err != nil {
// 			cm2.rmClient <- c
// 			return
// 		}
// 		switch req.t {
// 		case SCROLL_REQUEST:
// 			sc := flatgen.GetRootAsChatScrollRequest(req.b, 3).UnPack()
// 			go handleChatScroll2(sc, c.mch)
// 		case CHAT_EVENT:
// 			me := flatgen.GetRootAsMessageEvent(req.b, 3)
// 			cid := me.ChatId(nil)
// 			if me.Type() != 0x00 {
// 				cid.MutateTimestamp(utils.MakeTimestamp())
// 				cpbuf := make([]byte, len(me.Table().Bytes))
// 				copy(cpbuf, me.Table().Bytes)
// 				cm2.broadcast <- &chatMessage{dev: c.dev, room: c.room, buf: cpbuf}
// 				db.LDB.Put(msgIdValue(cid), cpbuf, nil)
// 			} else {
// 				// preparing sent and/or error buffer
// 				// which is [protocol_num(1byte), payloadlen(2bytes), payload]
// 				idlen := len(cid.Table().Bytes)
// 				resbuf := make([]byte, 3+(2*idlen))
// 				resbuf[0] = MESSAGE_SENT
// 				binary.BigEndian.PutUint16(resbuf[1:], uint16(idlen*2))
// 				copy(resbuf[3:], cid.Table().Bytes)
// 				cid.MutateTimestamp(utils.MakeTimestamp())
// 				copy(resbuf[3+idlen:], cid.Table().Bytes)

// 				mbuf := make([]byte, len(me.Table().Bytes))
// 				copy(mbuf, me.Table().Bytes)
// 				cm2.broadcast <- &chatMessage{dev: c.dev, room: c.room, buf: mbuf}

// 			}

// 			cid := me.ChatId(nil)
// 			// ogIdLen and newIdLen is the same, we only mutate the ts
// 			idLen = copy(resbuf[3:], cid.Table().Bytes)
// 			srvTs := utils.MakeTimestamp()
// 			cid.MutateTimestamp(srvTs)
// 			copy(resbuf[3+idLen:], cid.Table().Bytes)
// 			bcbuf := make([]byte, len(req.b))
// 			copy(bcbuf, req.b)
// 			cm2.broadcast <- &chatMessage{dev: c.dev, room: c.room, buf: bcbuf}
// 			if me.Type() == 0x00 {
// 				resbuf[0] = MESSAGE_SENT
// 				binary.BigEndian.PutUint16(resbuf[1:], uint16(idLen*2))
// 				sentbuf := make([]byte, 3+(idLen*2))
// 				copy(sentbuf, resbuf)
// 				c.mch <- sentbuf
// 			}
// 			if err := db.LDB.Put(msgIdValue(cid), req.b, nil); err != nil {
// 				log.Println("LDB.Put(MessageEvent) error:", err)
// 				resbuf[0] = MESSAGE_SAVE_ERROR
// 				errbuf := make([]byte, 3+(idLen*2))
// 				copy(errbuf, resbuf)
// 				c.mch <- errbuf
// 			}
// 		case NOTIFICATIONS:
// 			ntfs := flatgen.GetRootAsNotifications(req.b, 3)
// 			SendNotifications(ntfs)
// 		}
// 	}
// }
