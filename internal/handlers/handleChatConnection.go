package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
	// "github.com/gorilla/websocket"
)

const MAX_MSG_LEN int = 4096

func StartChatServer() {
	listener, err := net.Listen("tcp", ":11002")
	utils.Panic(err, "startChatServer error on net.Listen")
	defer listener.Close()
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
	rooms      []utils.Root
	iddev      *utils.Iddev
	conn       net.Conn
	sess       int64
	res        chan struct{}
	rbuf, cbuf []byte
}

type msgReq struct {
	sender *client
	cid    *flatgen.MessageId
	root   *utils.Root
	req    *Req
}

type connRequest struct {
	client *client
	root   *utils.Root
}

type Manager struct {
	conns      map[utils.Root]map[utils.Iddev]*client
	delClient  chan *client
	sw         chan *msgReq
	connect    chan *connRequest
	disconnect chan *connRequest
}

var ConnManager = Manager{
	conns:      make(map[utils.Root]map[utils.Iddev]*client),
	delClient:  make(chan *client),
	sw:         make(chan *msgReq),
	connect:    make(chan *connRequest),
	disconnect: make(chan *connRequest),
}

func sh(b []byte) []byte {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum(nil)
}

func (m *Manager) Run() {
	log.Println("M: running")
	for {
		log.Printf("==== CONNS PRINT ====")
		for root, room := range m.conns {
			log.Printf("\tROOT=%X\n", sh(root[:]))
			for iddev, client := range room {
				log.Printf("\t\tIDDEV=%X, client == nil -> %v\n",
					sh(iddev[:]), client == nil)
			}
		}

		select {
		// this is a request that comes from the client thread
		case del := <-m.delClient:
			log.Printf("MAN: del req for CLIENT=%X\n", sh(del.iddev[:]))
			log.Printf("MAN: CLIENT=%X has %d conns\n",
				sh(del.iddev[:]), len(del.rooms))
			for _, r := range del.rooms {
				if room := m.conns[r]; room != nil {
					c := room[*del.iddev]
					if c != nil && c.sess == del.sess {
						log.Printf("MAN: del CLIENT=%X from ROOT=%X\n",
							sh(c.iddev[:]), sh(r[:]))
						delete(room, *del.iddev)
						if len(room) == 0 {
							log.Printf("MAN: ROOT=%X empty, rmving",
								sh(r[:]))
							delete(m.conns, r)
						}
					}
				}
			}
			// the conn is potentially already closed
			// and it's not an issue, can call close on it multiple times
			log.Printf("MAN: freeing resources for CLIENT=%X\n", sh(del.iddev[:]))
			del.conn.Close()
			tools.OneKbPool.Put(del.rbuf)
			tools.FourKbPool.Put(del.cbuf)
			close(del.res)

		case sw := <-m.sw:
			// this can be used for timestamp and broadcast
			log.Printf("MAN: sw from CLIENT=%X for ROOT=%X\n",
				sh(sw.sender.iddev[:]), sh(sw.root[:]))
			if room := m.conns[*sw.root]; room != nil {
				for iddev, client := range room {
					log.Printf("MAN: CLIENT=%X is nil ? -> %v\n",
						iddev[:], client == nil)
				}

				id := flatgen.GetRootAsMessageEvent(sw.req.p, 0).ChatId(nil)
				if suff := id.Suffix(); suff == 0x00 || suff == 0x04 {
					handleChat(sw, room)
				} else {
					handleNonChat(sw, room)
				}
				sw.sender.res <- struct{}{}
			}

		case conn := <-m.connect:
			log.Printf("MAN: conn req from CLIENT=%X for ROOT=%X\n",
				sh(conn.client.iddev[:]), sh(conn.root[:]))
			if room := m.conns[*conn.root]; room == nil {
				log.Println("MAN: room doesn't exist, creating it")
				room = map[utils.Iddev]*client{*conn.client.iddev: conn.client}
				m.conns[*conn.root] = room
			} else if client := room[*conn.client.iddev]; client == nil {
				log.Println("MAN: adding the client to room")
				room[*conn.client.iddev] = conn.client
			} else if client.sess < conn.client.sess {
				log.Printf("MAN: client.sess: %d, newClient.sess: %d\n",
					client.sess, conn.client.sess)
				// replace
				log.Println("MAN: older session already connected, replacing")
				room[*conn.client.iddev] = conn.client
				// triggers a delClient request, which
				// will free the resources
				client.conn.Close()
			} else if client.sess == conn.client.sess {
				log.Println("MAN: already connected, ignoring")
			} else { // not a newer session
				log.Println("MAN: already a newer session connected")
				// destroy that new client request
				// ressources haven't been allocated yet
				conn.client.conn.Close()
			}
			conn.client.res <- struct{}{}

		// a client might want to disconnect from a root
		case disc := <-m.disconnect:
			log.Printf("MAN: disc req from CLIENT=%X for ROOT=%X\n",
				sh(disc.client.iddev[:]), sh(disc.root[:]))
			if room := m.conns[*disc.root]; room != nil {
				if client := room[*disc.client.iddev]; client != nil {
					if client.sess == disc.client.sess {
						log.Println("MAN: proceeding to removal")
						delete(room, *disc.client.iddev)

					}
				}
			}
			disc.client.res <- struct{}{}
		}
	}
}

type Req struct {
	t    byte
	l    uint16
	b, p []byte
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
	r.p = cbuf[3 : 3+r.l]
	return nil
}

// should I just send snips here?
func handleNonChat(sw *msgReq, room map[utils.Iddev]*client) {
	log.Printf("MAN: handleing non-chat from CLIENT=%X in ROOM=%X\n",
		sh(sw.sender.iddev[:]), sh(sw.root[:]))
	sw.cid.MutateTimestamp(utils.MakeTimestamp())
	for iddev, o := range room {
		if iddev != *sw.sender.iddev {
			log.Printf("MAN: writing non-chat to CLIENT=%X for ROOM=%X\n",
				sh(iddev[:]), sh(sw.root[:]))
			o.conn.Write(sw.req.b)
		}
	}
	buf := bytes.NewBuffer(sw.sender.rbuf[:0])
	utils.WriteMsgId(buf, sw.cid)
	db.LV.Put(buf.Bytes(), sw.req.b, nil)
}

func handleChat(sw *msgReq, room map[utils.Iddev]*client) {
	const fullLen = 1 + 2 + utils.RAW_MSG_ID_LEN*2
	log.Printf("MAN: handleing chat from CLIENT=%X in ROOM=%X\n",
		sh(sw.sender.iddev[:]), sh(sw.root[:]))
	buf := bytes.NewBuffer(sw.sender.rbuf[1+2 : 1+2])
	idLen := utils.WriteMsgId(buf, sw.cid)
	sw.cid.MutateTimestamp(utils.MakeTimestamp())
	if sw.cid.Suffix() == 0x00 { // this is unsent chat suffix
		sw.cid.MutateSuffix(0x03) // this is a sent id suffix
	}

	utils.WriteMsgId(buf, sw.cid)

	freshId := sw.sender.rbuf[1+2+idLen : fullLen]

	sw.sender.rbuf[0] = MESSAGE_SENT
	binary.BigEndian.PutUint16(sw.sender.rbuf[1:], uint16(idLen*2))
	for iddev, o := range room {
		if iddev != *sw.sender.iddev {
			log.Printf("MAN: writing CHAT=%X to CLIENT=%X in ROOM=%X\n",
				sh(freshId), sh(iddev[:]), sh(sw.root[:]))

			log.Println("o == nil", o == nil)

			o.conn.Write(sw.req.b)
		} else {
			log.Printf("MAN: writing SENT=%X to CLIENT=%X in ROOM=%X\n",
				sh(freshId), sh(iddev[:]), sh(sw.root[:]))
			sw.sender.conn.Write(sw.sender.rbuf[:fullLen])
		}
	}

	if err := db.LV.Put(freshId, sw.req.b, nil); err != nil {
		log.Printf("MAN: LDB.Put(MessageEvent) in ROOM=%X error=%v\n",
			sh(sw.root[:]), err)
		sw.sender.rbuf[0] = MESSAGE_SAVE_ERROR
		sw.sender.conn.Write(sw.sender.rbuf[:fullLen])
	}
}

func HandleChatConn(conn net.Conn) {
	iddev := utils.Iddev{}
	if _, err := io.ReadFull(conn, iddev[:]); err != nil {
		log.Println("HandleChatConn3: error reading iddev:", err)
		conn.Close()
		return
	}
	c := &client{iddev: &iddev,
		conn:  conn,
		sess:  utils.MakeTimestamp(),
		cbuf:  tools.FourKbPool.Get(),
		rbuf:  tools.OneKbPool.Get(),
		rooms: make([]utils.Root, 0, 5),
		res:   make(chan struct{})}
	go c.readFromClientSync()

}

// if we want to spread horizontally, make more managers
// based on the hash of a root
// (it could be a good idea, but would need benchmarking)
// (there's not limit to horizontal spreading this way,
// but is there a reason to do so? maybe split by the amount of cpu on the machine)
// we don't know...
func (c *client) readFromClientSync() {
	var (
		req = &Req{} // holds the request
		// buffer to write roots
		root    utils.Root
		rootBuf = bytes.NewBuffer(root[:0])
	)

	defer log.Printf("CLIENT=%X: Thread was closed\n", sh(c.iddev[:]))
	for {
		log.Printf("CLIENT=%X: reading from client\n", sh(c.iddev[:]))
		log.Printf("CLIENT=%X: conns:\n", sh(c.iddev[:]))
		for _, x := range c.rooms {
			log.Printf("\t%X\n", x[:])
		}

		// sw req -> close() -> close(sent) -> sw sent
		// this sequence could theoratically crash the server
		// so manager should be responsible for freeing client resources
		if err := readReq(c.conn, req, c.cbuf); err != nil {
			ConnManager.delClient <- c
			return
		}

		log.Printf("CLIENT=%X\nreq:\nt:%d\nl:%d\np:%x\n",
			sh(c.iddev[:]), req.t, req.l, req.p)

		switch req.t {
		case CHAT_CONN_REQ:
			log.Printf("CLIENT=%X: [BEGIN] chat conn request\n", sh(c.iddev[:]))
			copy(root[:], req.p)
			if utils.Contains(root, c.rooms) {
				log.Printf("CLIENT=%X: already connected to ROOT=%X\n",
					sh(c.iddev[:]), root[:])
				continue
			}
			ConnManager.connect <- &connRequest{client: c, root: &root}
			<-c.res
			c.rooms, _ = utils.AddToSet(root, c.rooms)
			c.rbuf[0] = 0x16
			binary.BigEndian.PutUint16(c.rbuf[1:], uint16(len(root)))
			copy(c.rbuf[3:], root[:])
			c.conn.Write(c.rbuf[:3+utils.RAW_ROOT_ID_LEN])
			log.Printf("CLIENT=%X: [DONE] chat conn request\n", sh(c.iddev[:]))

		case CHAT_DISC_REQ:
			log.Printf("CLIENT=%X: [BEGIN] chat disc request\n", sh(c.iddev[:]))
			copy(root[:], req.p)
			if !utils.Contains(root, c.rooms) {
				log.Printf("CLIENT=%X: already disconnected to ROOT=%X\n",
					sh(c.iddev[:]), root[:])
				continue
			}
			ConnManager.disconnect <- &connRequest{client: c, root: &root}
			<-c.res
			c.rooms, _ = utils.Remove(root, c.rooms)
			c.rbuf[0] = 0x17
			binary.BigEndian.PutUint16(c.rbuf[1:], uint16(len(root)))
			copy(c.rbuf[3:], root[:])
			c.conn.Write(c.rbuf[:3+utils.RAW_ROOT_ID_LEN])
			log.Printf("CLIENT=%X: [DONE] chat disc request\n", sh(c.iddev[:]))

		case SCROLL_REQUEST:
			log.Printf("CLIENT=%X: [BEGIN] scroll request\n", sh(c.iddev[:]))
			before := req.p[0] == 0x01
			limit := binary.BigEndian.Uint16(req.p[1:])
			ref := req.p[3:]
			log.Printf(`
before: %v
limit:  %d
ref:    %x
`, before, limit, ref)

			if before {
				handleChatScrollBeforeSync3(req.p[3:], c, limit)
			} else {
				handleChatScrollAfterSync3(req.p[3:], c, limit)
			}
			log.Printf("CLIENT=%x: [DONE] scroll request\n", sh(c.iddev[:]))

		case CHAT_EVENT:
			log.Printf("CLIENT=%X: [BEGIN] chat event\n", sh(c.iddev[:]))
			cid := flatgen.GetRootAsMessageEvent(req.p, 0).ChatId(nil)
			utils.WriteRoot(rootBuf, cid.Root(nil))
			rootBuf.Reset()
			ConnManager.sw <- &msgReq{sender: c, req: req, root: &root, cid: cid}
			<-c.res
			log.Printf("CLIENT=%X: [DONE] chat event\n", sh(c.iddev[:]))

		case NOTIFICATIONS:
			log.Printf("CLIENT=%X: [BEGIN] notification\n", sh(c.iddev[:]))
			ntfs := flatgen.GetRootAsNotifications(req.p, 0)
			SendNotifications(ntfs)
			log.Printf("CLIENT=%X: [DONE] notification\n", sh(c.iddev[:]))
		}
	}
}

// func (c *client3) freeResources() {
// 	c.conn.Close()
// 	tools.FourKbPool.Put(c.cbuf)
// 	tools.OneKbPool.Put(c.rbuf)
// 	log.Printf("CLIENT=%x WAS SHUTDOWN\n", c.iddev)
// }

// type room2 struct {
// 	root    string
// 	sw      chan *SyncWrite
// 	add, rm chan *client2
// 	// key is a iddev
// 	clients map[string]*client2
// }

// type room3 struct {
// 	root    utils.Root
// 	sw      chan *SyncWrite3
// 	add, rm chan *client3
// 	// key is a iddev
// 	clients map[utils.Iddev]*client3
// }

// type roomMan struct {
// 	rooms map[utils.Root]*room3
// 	del   chan *struct {
// 		room *room3
// 		res  chan struct{}
// 	}
// 	get chan *struct {
// 		root utils.Root
// 		res  chan *room3
// 	}
// }

// type Cm2 struct {
// 	rooms map[string]*room2
// 	rmr   chan *room2
// 	add   chan *client2
// 	//, rm chan *client2
// }

// var ChatConnsManager = &Cm2{
// 	rooms: make(map[string]*room2),
// 	add:   make(chan *client2),
// 	// rm:    make(chan *client2),
// }

// type SyncWrite struct {
// 	sender *client2
// 	req    *Req
// }

// type SyncWrite3 struct {
// 	sender *client3
// 	req    *Req
// 	sent   chan struct{}
// 	rbuf   []byte
// }

// type client2 struct {
// 	sent        chan struct{}
// 	room        *room2
// 	sess        int64
// 	iddev, root string
// 	conn        net.Conn
// 	cbuf, rbuf  []byte
// }

// func (c *client2) shutdown() {
// 	c.conn.Close()
// 	tools.FourKbPool.Put(c.cbuf)
// 	tools.OneKbPool.Put(c.rbuf)
// 	close(c.sent)
// }

// type brochan = chan *struct {
// 	req *utils.Root
// 	res chan<- *room2
// }

// messages requests only come after room conn
// but is this really safe enough?
// we don't confirm conns on client side...
// clearly a conn could fail, and a message request ensue...
// well, client can hold a pointer to rooms (successfull conn)
// yes, that seems to work fine.
// if we don't have a pointer, we request one
// type roomConn struct {
// 	// iddev *utils.Iddev
// 	root *utils.Root
// 	res  chan *room3
// }

// type clientMan struct {
// 	clients             map[utils.Iddev]*client3
// 	rooms               map[utils.Root]*room3
// 	roomRequest         chan *roomConn
// 	rmRoom              chan *room3
// 	addClient, rmClient chan *client3
// }

// var ClientMan = clientMan{
// 	clients:     make(map[utils.Iddev]*client3),
// 	rooms:       make(map[utils.Root]*room3),
// 	roomRequest: make(chan *roomConn),
// 	rmRoom:      make(chan *room3),
// 	addClient:   make(chan *client3),
// 	rmClient:    make(chan *client3),
// }

// func (cm *clientMan) Run() {
// 	for {
// 		select {
// 		case newClient := <-cm.addClient:
// 			log.Printf("CM: new client iddev=%x\n", newClient.iddev)
// 			if oldClient := cm.clients[newClient.iddev]; oldClient != nil {
// 				log.Println("CM: client already exists")
// 				if oldClient.sess < newClient.sess {
// 					log.Println("CM: newer session, replacing")
// 					// since oldClient is in the map, it's running
// 					// we want to shut it down, this closes its thread
// 					oldClient.close <- struct{}{}
// 					//oldClient.freeResources()
// 					cm.clients[newClient.iddev] = newClient
// 					go newClient.readFromClientSync3()
// 				} else {
// 					// don't know if that's possible, but if so
// 					// we are getting a request from an older session
// 					// we just close it, it's not running
// 					log.Println("CM: older session, canceling")
// 					close(newClient.close)
// 					newClient.conn.Close()
// 					// newClient.freeResources()
// 				}
// 			} else {
// 				log.Println("CM: client doesn't exists, adding it")
// 				cm.clients[newClient.iddev] = newClient
// 				go newClient.readFromClientSync3()
// 			}

// 		// it's possible to get a remove request client
// 		// from someone's that is not in the clients map?
// 		case rmClient := <-cm.rmClient:
// 			log.Printf("CM: remove request for client iddev=%x\n", rmClient.iddev)
// 			curClient := cm.clients[rmClient.iddev]
// 			if curClient != nil && curClient.sess == rmClient.sess {
// 				log.Printf("CM: removing iddev=%x from clients\n",
// 					rmClient.iddev)
// 				// rmClient.freeResources()
// 				delete(cm.clients, rmClient.iddev)
// 			}

// 		case rc := <-cm.roomRequest:
// 			log.Printf("CM: room request root=%x\n", *rc.root)
// 			var r *room3
// 			if r = cm.rooms[*rc.root]; r == nil {
// 				log.Printf("CM: creating room root=%x\n", *rc.root)
// 				r = &room3{
// 					root:    *rc.root,
// 					sw:      make(chan *SyncWrite3),
// 					add:     make(chan *client3),
// 					rm:      make(chan *client3),
// 					clients: make(map[utils.Iddev]*client3)}
// 				go r.run()
// 				cm.rooms[*rc.root] = r
// 			}
// 			rc.res <- r

// 		case rr := <-cm.rmRoom:
// 			log.Printf("CM: removing room=%x\n", rr.root)
// 			delete(cm.rooms, rr.root)
// 			// rr.shutdown()
// 		}
// 	}
// }

// func (r *roomMan) Run() {
// 	for {
// 		select {
// 		// this is a request that comes from
// 		// an empty room
// 		case delRoom := <-r.del:
// 			delete(r.rooms, delRoom.room.root)
// 			delRoom.res <- struct{}{}

// 		case roomReq := <-r.get:
// 			room := r.rooms[roomReq.root]
// 			if room == nil {
// 				room = &room3{
// 					root:    *&roomReq.root,
// 					sw:      make(chan *SyncWrite3),
// 					add:     make(chan *client3),
// 					rm:      make(chan *client3),
// 					clients: make(map[utils.Iddev]*client3)}
// 				room.run()
// 			}
// 			roomReq.res <- room
// 		}
// 	}
// }

// func (room *room3) shutdown() {
// 	close(room.sw)
// 	close(room.add)
// 	close(room.rm)
// 	log.Printf("ROOM=%x WAS SHUTDOWN\n", room.root)
// }

// func (room *room2) shutdown() {
// 	close(room.sw)
// 	close(room.add)
// 	close(room.rm)
// }

// func (room *room2) run() {
// 	for {
// 		select {
// 		case w := <-room.sw:
// 			log.Println("message request from", w.sender.iddev)
// 			me := flatgen.GetRootAsMessageEvent(w.req.b, 3)
// 			cid := me.ChatId(nil)
// 			if me.Type() == 0x00 {
// 				handleChat(cid, w.sender, w.req)
// 			} else {
// 				handleNonChat(cid, w.sender, w.req)
// 			}
// 			w.sender.sent <- struct{}{}

// 		case a := <-room.add:
// 			if c := room.clients[a.iddev]; c != nil && c.sess < a.sess {
// 				log.Printf("client iddev=%s, already connected, replacing it\n",
// 					a.iddev)
// 				c.shutdown()
// 				delete(room.clients, c.iddev)
// 			}

// 			log.Printf("adding client iddev=%s\n", a.iddev)
// 			room.clients[a.iddev] = a
// 			a.room = room
// 			go a.readFromClientSync()

// 		case r := <-room.rm:
// 			log.Printf("removing client iddev=%s\n", r.iddev)
// 			r.shutdown()
// 			delete(room.clients, r.iddev)
// 			if len(room.clients) == 0 {
// 				log.Printf("room root=%s is empty, requesting removal\n",
// 					room.root)
// 				ChatConnsManager.rmr <- room
// 				return
// 			}
// 		}
// 	}
// }

// func (room *room3) run() {
// 	log.Printf("ROOM=%x: running\n", room.root)
// 	defer log.Printf("ROOM=%x: Thread was closed\n", room.root)

// 	for {
// 		select {
// 		case w := <-room.sw:
// 			log.Printf("ROOM=%x: message request from=%x\n",
// 				room.root, w.sender.iddev)
// 			me := flatgen.GetRootAsMessageEvent(w.req.b, 3)
// 			cid := me.ChatId(nil)
// 			if me.Type() == 0x00 {
// 				handleChat3(w.sender, cid, room, w.rbuf, w.req)
// 			} else {
// 				handleNonChat3(w.sender, cid, room, w.rbuf, w.req)
// 			}
// 			w.sent <- struct{}{}

// 		case a := <-room.add:
// 			log.Printf("ROOM=%x: add request for CLIENT=%x\n", room.root, a.iddev)
// 			c := room.clients[a.iddev]
// 			if c != nil && c.sess < a.sess {
// 				log.Printf("ROOM=%x: CLIENT=%x newer session, replacing\n",
// 					room.root, a.iddev)
// 				c.freeResources()
// 				delete(room.clients, c.iddev)
// 			} else if c != nil && c.sess == a.sess {
// 				log.Printf("ROOM=%x, CLIENT=%x already connected, ignoring\n")
// 			} else if c == nil {
// 				log.Printf("ROOM=%x: adding CLIENT=%x\n", room.root, a.iddev)
// 				room.clients[a.iddev] = a
// 				// a.rooms[room.root] = room
// 			}

// 		case r := <-room.rm:
// 			log.Printf("ROOM=%x: removing CLIENT=%x\n", room.root, r.iddev)
// 			delete(room.clients, r.iddev)
// 			if len(room.clients) == 0 {
// 				log.Printf("ROOM=%x: empty room removing\n", room.root)
// 				close(room.sw)
// 				close(room.rm)
// 				close(room.add)
// 				ClientMan.rmRoom <- room
// 				return
// 			}
// 		}
// 	}
// }

// is it a problem if
// it can be a problem, we want the ts and save to be in sync
// func handleNonChat(cid *flatgen.MessageId, c *client2, req *Req) {
// 	log.Println("handleing non-chat")
// 	cid.MutateTimestamp(utils.MakeTimestamp())
// 	for dev, o := range c.room.clients {
// 		if dev != c.iddev {
// 			log.Println("writing non-chat to dev:", dev)
// 			o.conn.Write(req.b)
// 		}
// 	}
// 	buf := bytes.NewBuffer(c.rbuf[:0])
// 	utils.WriteMsgId(buf, cid)
// 	db.LV.Put(buf.Bytes(), req.b, nil)
// }

// func handleChat(cid *flatgen.MessageId, c *client2, req *Req) {
// 	log.Println("handleing chat")
// 	buf := bytes.NewBuffer(c.rbuf[1+2 : 1+2])
// 	idLen := utils.WriteMsgId(buf, cid)
// 	cid.MutateTimestamp(utils.MakeTimestamp())
// 	cid.MutateSuffix(0x03) // this is a sent id suffix
// 	utils.WriteMsgId(buf, cid)

// 	fullLen := 1 + 2 + idLen*2
// 	freshId := c.rbuf[1+2+idLen : fullLen]

// 	c.rbuf[0] = MESSAGE_SENT
// 	binary.BigEndian.PutUint16(c.rbuf[1:], uint16(idLen*2))
// 	for iddev, o := range c.room.clients {
// 		if iddev != c.iddev {
// 			log.Printf("writing chat: %x to iddev: %s\n", freshId, iddev)
// 			o.conn.Write(req.b)
// 		} else {
// 			log.Printf("writing sent: %x to iddev: %s\n", freshId, iddev)
// 			c.conn.Write(c.rbuf[:fullLen])
// 		}
// 	}

// 	if err := db.LV.Put(freshId, req.b, nil); err != nil {
// 		log.Println("LDB.Put(MessageEvent) error:", err)
// 		c.rbuf[0] = MESSAGE_SAVE_ERROR
// 		c.conn.Write(c.rbuf[:fullLen])
// 	}
// }

// func handleNonChat3(c *client3, cid *flatgen.MessageId, room *room3, rbuf []byte, req *Req) {
// 	log.Printf("ROOM=%x: handleing non-chat", room.root)
// 	cid.MutateTimestamp(utils.MakeTimestamp())
// 	for iddev, o := range room.clients {
// 		if iddev != *c.iddev {
// 			log.Printf("ROOM=%x: writing non-chat to CLIENT=%x\n", room.root, iddev)
// 			o.conn.Write(req.b)
// 		}
// 	}
// 	buf := bytes.NewBuffer(rbuf[:0])
// 	utils.WriteMsgId(buf, cid)
// 	db.LV.Put(buf.Bytes(), req.b, nil)
// }

// func handleChat3(c *client3, cid *flatgen.MessageId, room *room3, rbuf []byte, req *Req) {
// 	log.Printf("ROOM=%x: handleing chat", room.root)
// 	buf := bytes.NewBuffer(rbuf[1+2 : 1+2])
// 	idLen := utils.WriteMsgId(buf, cid)
// 	cid.MutateTimestamp(utils.MakeTimestamp())
// 	cid.MutateSuffix(0x03) // this is a sent id suffix
// 	utils.WriteMsgId(buf, cid)

// 	fullLen := 1 + 2 + idLen*2
// 	freshId := rbuf[1+2+idLen : fullLen]

// 	rbuf[0] = MESSAGE_SENT
// 	binary.BigEndian.PutUint16(rbuf[1:], uint16(idLen*2))
// 	for iddev, o := range room.clients {
// 		if iddev != *c.iddev {
// 			log.Printf("ROOM=%x: writing chat=%x to CLIENT=%x\n",
// 				room.root, freshId, iddev)
// 			o.conn.Write(req.b)
// 		} else {
// 			log.Printf("ROOM=%x: writing sent=%x to CLIENT=%x\n",
// 				room.root, freshId, iddev)
// 			c.conn.Write(rbuf[:fullLen])
// 		}
// 	}

// 	if err := db.LV.Put(freshId, req.b, nil); err != nil {
// 		log.Printf("ROOM=%x: LDB.Put(MessageEvent) error=%v\n", room.root, err)
// 		rbuf[0] = MESSAGE_SAVE_ERROR
// 		c.conn.Write(rbuf[:fullLen])
// 	}
// }

// func HandleChatConn(conn net.Conn) {
// 	cbuf := tools.FourKbPool.Get()
// 	root, iddev, err := parseConnReq(conn, cbuf)
// 	if err != nil {
// 		tools.FourKbPool.Put(cbuf)
// 		conn.Close()
// 		return
// 	}
// 	sess := utils.MakeTimestamp()
// 	rbuf := tools.OneKbPool.Get()
// 	sent := make(chan struct{})
// 	c := &client2{sent: sent, iddev: iddev, root: root,
// 		sess: sess, conn: conn, cbuf: cbuf, rbuf: rbuf}
// 	ChatConnsManager.add <- c
// }

// func parseConnReq(conn net.Conn, cbuf []byte) (string, string, error) {
// 	const iddevLen = utils.RAW_IDDEV_LEN
// 	const rootLen = utils.RAW_ROOT_ID_LEN
// 	var err error
// 	if _, err = io.ReadFull(conn, cbuf[:iddevLen]); err != nil {
// 		return "", "", err
// 	}
// 	iddev := hex.EncodeToString(cbuf[:iddevLen])
// 	if _, err = io.ReadFull(conn, cbuf[:rootLen]); err != nil {
// 		return "", "", err
// 	}
// 	root := hex.EncodeToString(cbuf[:rootLen])
// 	log.Printf("new chat connection, root=%s, iddev=%s\n", root, iddev)
// 	return root, iddev, nil
// }

// func parseConnReq3(conn net.Conn, cbuf []byte) error {
// 	var iddev utils.Iddev
// 	var err error
// 	if _, err = io.ReadFull(conn, cbuf[:iddevLen]); err != nil {
// 		return nil, err
// 	}
// 	iddev := hex.EncodeToString(cbuf[:iddevLen])
// 	if _, err = io.ReadFull(conn, cbuf[:rootLen]); err != nil {
// 		return "", "", err
// 	}
// 	root := hex.EncodeToString(cbuf[:rootLen])
// 	log.Printf("new chat connection, root=%s, iddev=%s\n", root, iddev)
// 	return root, iddev, nil
// }

// func (chatMan *Cm2) Run() {
// 	for {
// 		select {
// 		case c := <-chatMan.add:
// 			log.Printf("[ADD] root=%s, iddev=%s, sess=%d\n",
// 				c.root, c.iddev, c.sess)
// 			if r := chatMan.rooms[c.root]; r == nil {
// 				log.Println("Room doesn't exists, creating it!")
// 				clients := make(map[string]*client2)
// 				// clients[c.iddev] = c
// 				add, rm := make(chan *client2), make(chan *client2)
// 				sw := make(chan *SyncWrite)
// 				room := &room2{root: c.root, clients: clients,
// 					add: add, rm: rm, sw: sw}
// 				go room.run()
// 				room.add <- c
// 			} else {
// 				log.Printf("Room exists, adding client")
// 				r.add <- c
// 			}
// 			log.Printf("Client iddev=%s added to room=%s\n", c.iddev, c.root)

// 		case rmr := <-chatMan.rmr:
// 			log.Printf("Removing room root=%s\n", rmr.root)
// 			delete(chatMan.rooms, rmr.root)
// 			rmr.shutdown()
// 		}
// 	}
// }

// func (c *client2) readFromClientSync() {
// 	var (
// 		// me  *flatgen.MessageEvent
// 		// cid *flatgen.MessageId
// 		req = &Req{}
// 	)

// 	for {
// 		log.Println("reading from client")
// 		if err := readReq(c.conn, req, c.cbuf); err != nil {
// 			log.Printf("error reading from client iddev=%s, closing", c.iddev)
// 			c.room.rm <- c
// 			return
// 		}
// 		log.Printf("req:\nt:%d\nl:%d\np:%x\n", req.t, req.l, req.p)
// 		// log.Printf("req:\nt:%d\nl:%d\nb:%x\nfull:%x\n", req.t, req.l, req.b, c.cbuf)
// 		switch req.t {
// 		case SCROLL_REQUEST:
// 			// both before and after scrolls can be active at the same time
// 			// but there is only one of a kind at a time (handled from client side)
// 			// but it can be concurrent with read from client
// 			// -- alternative would be to by sync with read from client
// 			// -- would probably be good as well
// 			log.Println("received scroll request")
// 			before := req.p[0] == 0x01
// 			if before {
// 				handleChatScrollBeforeSync2(req.p[1:], c)
// 			} else {
// 				handleChatScrollAfterSync2(req.p[1:], c)
// 			}
// 		case CHAT_EVENT:
// 			log.Println("received chat event")
// 			// we synchronize sending messages on a room basis
// 			// this assures order
// 			c.room.sw <- &SyncWrite{sender: c, req: req}
// 			<-c.sent // sent request depends on client buffer, so we wait
// 			// until the message is sent before reading to other reqs

// 		case NOTIFICATIONS:
// 			log.Println("received notification")
// 			ntfs := flatgen.GetRootAsNotifications(req.p, 0)
// 			SendNotifications(ntfs)
// 		}
// 	}
// }

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
