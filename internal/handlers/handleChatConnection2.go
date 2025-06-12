package handlers

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils/id_utils"
	"github.com/coldstar-507/utils/utils"
)

type iddev = id_utils.Iddev_
type nodeid = id_utils.NodeId
type _root = id_utils.Root
type msgid = id_utils.MsgId

type conns = map[iddev]*client2
type chats = map[_root]*sconns

type sconns = utils.Smap[iddev, *client2]
type schats = utils.Smap[_root, *sconns]

var n_man uint32
var n_bc_sem uint32
var n_db_sem uint32

var mans []*manager2

const MAX_MSG_LEN2 int = 4096

// max concurrent db write request PER manager
// should probably be a factor of nMan
// so we always have the same MAX_N_SEM

func loadConfig() {
	const (
		nChatKey  = "N_CHAT_MANAGERS"
		nDbSemKey = "N_DB_SEMAPHORE"
		nBcSemKey = "N_BC_SEMAPHORE"
	)

	_nMan, err := strconv.Atoi(os.Getenv(nChatKey))
	nMan := uint32(_nMan)
	utils.Panic(err, "loadConfig: ENV: undefined %s", nChatKey)
	utils.Assert(nMan > 0, "loadConfig: %s must be a positive u32: %d", nChatKey, nMan)
	n_man = nMan

	_nDbSem, err := strconv.Atoi(os.Getenv(nDbSemKey))
	nDbSem := uint32(_nDbSem)
	utils.Panic(err, "loadConfig: ENV: undefined %s", nDbSemKey)
	utils.Assert(nDbSem > 0, "loadConf: %s must be a positive u32: %d", nDbSemKey, nDbSem)
	n_db_sem = nDbSem

	_nBcSem, err := strconv.Atoi(os.Getenv(nBcSemKey))
	nBcSem := uint32(_nBcSem)
	utils.Panic(err, "loadConfig: ENV: undefined %s", nBcSemKey)
	utils.Assert(nBcSem > 0, "loadConf: %s must be a positive u32: %d", nBcSemKey, nBcSem)
	n_bc_sem = nBcSem
}

func printMans() {
	for i, m := range mans {
		fmt.Printf("man %d:\n", i)
		m.schats.Do(func(key _root, value *sconns) {
			fmt.Printf("\troot %x\n", key)
			value.Dok(func(id iddev) {
				fmt.Printf("\t\tiddev %x\n", id)
			})
		})
	}
}

func (m *manager2) runSyncer() {
	for req := range m.syncer {
		cid := flatgen.GetRootAsMessageEvent(req.mbuf, 0).ChatId(nil)
		cid.MutateTimestamp(utils.MakeTimestamp())
		isChat := cid.Suffix() == byte(0x00)
		if isChat {
			cid.MutateSuffix(0x03)
		}
		req.res <- isChat // timestamp conf, can proceed to broadcast
		isSnip := cid.Suffix() == byte(0x04)
		q := fmt.Sprintf(`INSERT INTO db_one.%s (root, ts, nonce, msg)
                                  VALUES (?, ?, ?, ?)`, If(isSnip, "snips", "messages"))
		werr := db.Scy.Query(q, req.root[:], cid.Timestamp(), cid.U32(), req.mbuf).Exec()
		if werr != nil {
			req.res <- false
		} else {
			req.res <- true
		}
	}
}

func initChatConnsManagers2() {
	mans = make([]*manager2, n_man)
	for i := range n_man {
		man := &manager2{
			i:      i,
			schats: &schats{M: make(chats)},
			dbsem:  make(chan struct{}, n_db_sem),
			bcsem:  make(chan struct{}, n_bc_sem),
			syncer: make(chan *msg2),
		}
		mans[i] = man
		go man.runSyncer()
	}
}

func idf(root _root) uint32 {
	h := fnv.New32()
	h.Write(root[:])
	return h.Sum32() % n_man
}

func GetMan(root _root) *manager2 {
	i := idf(root)
	return mans[i]
}

func GetChats(root _root) *schats {
	i := idf(root)
	return mans[i].schats
}
func StartChatServer2() {
	listener, err := net.Listen("tcp", ":11002")
	utils.Panic(err, "StartChatServer error on net.Listen")
	defer listener.Close()
	loadConfig()
	initChatConnsManagers2()

	go func() {
		ticker := time.NewTicker(time.Second * 20)
		for range ticker.C {
			printMans()
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("error accepting connection:", err)
		} else {
			fmt.Println("new chat connection:", conn.LocalAddr())
		}
		go HandleChatConn2(conn)
	}
}

type manager2 struct {
	i      uint32
	schats *schats
	dbsem  chan struct{}
	bcsem  chan struct{}
	syncer chan *msg2
}

type client2 struct {
	rooms []_root
	iddev iddev
	conn  *utils.ClientConn
	sess  int64
}

type msg2 struct {
	mbuf []byte
	root _root
	pre  msgid
	res  chan bool
}

// we send MESSAGE_SENT | LEN | OLD_ID | NEW_ID to sender
// we send CHAT_EVENT | LEN | MSG to conns (msg has the mutated id)
// we send MESSAGE_SAVE_ERROR | LEN | NEW_ID to sender if can't save
func handleChatEvent(sender *client2, sw *msg2, conf bool) {
	m := GetMan(sw.root)
	fmt.Printf("man%d: handleing chat from client=%x in room=%x\n",
		m.i, sender.iddev, sw.root)
	m.syncer <- sw
	wg := sync.WaitGroup{}
	var cs []*client2
	m.schats.ReadingAt(sw.root, func(e *sconns) {
		e.Reading(func(e conns) {
			cs = make([]*client2, 0, len(e))
			for _, c := range e {
				cs = append(cs, c)
			}
		})
	})

	isChat := <-sw.res
	for _, c := range cs {
		m.bcsem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-m.bcsem }()
			if c.iddev != sender.iddev {
				c.conn.WriteBin(CHAT_EVENT, uint16(len(sw.mbuf)), sw.mbuf)
			}
		}()
	}

	// if it's a chat, send to sender the 'sent' chat id
	if isChat && conf {
		wg.Add(1)
		m.bcsem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-m.bcsem }()
			sentId := flatgen.GetRootAsMessageEvent(sw.mbuf, 0).ChatId(nil)
			sender.conn.Locked(func(w io.Writer) {
				l := uint16(2 * id_utils.RAW_MSG_ID_LEN)
				utils.WriteBin(w, MESSAGE_SENT, l, sw.pre[:])
				id_utils.WriteMsgId(w, sentId)
			})
		}()
	}

	wg.Wait()
	<-sw.res // database write, we wait for it because it depends on msgbuffer

}

func HandleChatConn2(conn net.Conn) {
	iddev := iddev{}
	if _, err := io.ReadFull(conn, iddev[:]); err != nil {
		fmt.Println("HandleChatConn: error reading iddev:", err)
		conn.Close()
		return
	}
	c := &client2{
		iddev: iddev,
		conn:  utils.NewLockedConn(conn),
		sess:  utils.MakeTimestamp(),
		rooms: make([]_root, 0, 5),
	}
	c.readFromClientSync_2()
}

func connect(c *client2, root _root) {
	fmt.Printf("connecting cl=%x to rt=%x\n", c.iddev, root)
	man := GetChats(root)
	success := man.ReadingAt(root, func(e *sconns) {
		fmt.Printf("swapping cl=%x\n", c.iddev)
		if v, swapped := e.Swap(c.iddev, c); swapped {
			fmt.Printf("was swapped cl=%x\n", c.iddev)
			v.conn.C.Close()
		}
	})

	if !success {
		fmt.Println("failed")
		man.ModifyingAt(root, func(s *sconns) {
			fmt.Println("adding")
			s.M[c.iddev] = c
		}, func() *sconns {
			fmt.Println("creating")
			sc := &sconns{M: make(conns)}
			sc.M[c.iddev] = c
			return sc
		})
	}
	c.rooms, _ = utils.AddToSet(root, c.rooms)
	c.conn.WriteBin(byte(0x16), uint16(len(root)), root[:])
}

func disconnect(c *client2, root _root) {
	var clearRoom bool
	chats := GetChats(root)
	chats.ReadingAt(root, func(e *sconns) {
		clearRoom = e.Delete(c.iddev) == 0
	})
	if clearRoom {
		chats.DeleteIf(root, func(value *sconns) bool {
			return len(value.M) == 0
		})
	}
	c.rooms, _ = utils.Remove(root, c.rooms)
	c.conn.WriteBin(byte(0x17), uint16(len(root)), root[:])
}

func (c *client2) readFromClientSync_2() {
	// stack values
	var (
		reqType  byte
		reqLen   uint16
		isBefore bool
		limit    uint16
		isSnips  bool
		ts       int64
		msgBuf   = [MAX_MSG_LEN2]byte{}
		root     = _root{}
		nodeId   = nodeid{}
		pre      = msgid{}
		rootBuf  = bytes.NewBuffer(root[:0])
		preBuf   = bytes.NewBuffer(pre[:0])
		res      = make(chan bool)
		err      error
	)

	defer fmt.Printf("cl=%x: thread was closed\n", c.iddev[:])

	go func() {
		ticker := time.NewTicker(time.Second * 20)
		defer fmt.Printf("killed heartbeater for %x, sess=%d\n", c.iddev, c.sess)
		defer ticker.Stop()
		var heartbeatErr error
		for {
			<-ticker.C
			heartbeatErr = c.conn.WriteBin(byte(0x99))
			if heartbeatErr != nil {
				fmt.Printf("cl=%x, sess=%d heartbeat err: %v\n",
					c.iddev, c.sess, heartbeatErr)
				c.conn.C.Close()
				return
			}
			fmt.Printf("cl=%x, sess=%d, heartbeat\n", c.iddev, c.sess)
		}
	}()

	for {
		fmt.Printf("cl=%x: reading from client\n", c.iddev)
		fmt.Printf("cl=%x: conns:\n", c.iddev)
		for _, x := range c.rooms {
			fmt.Printf("\t%x\n", x)
		}

		err = utils.ReadBin(c.conn.C, &reqType, &reqLen)
		if err != nil {
			fmt.Printf("cl=%x:, sess=%d, readBin error=%v, closing client\n",
				c.iddev, c.sess, err)

			m := utils.SplitMap(c.rooms, idf)
			for i, roots := range m {
				man := mans[i]
				var md []_root
				man.schats.Reading(func(e chats) {
					for _, root := range roots {
						v, ok := e[root]
						if ok && v.Delete(c.iddev) == 0 {
							md = append(md, root)
						}
					}
				})

				if len(md) > 0 {
					man.schats.Modifying(func(e chats) {
						for _, root := range md {
							v, ok := e[root]
							if ok && len(v.M) == 0 {
								delete(e, root)
							}
						}
					})
				}
			}

			close(res)
			return
		}

		switch reqType {
		case CHAT_CONN_REQ:
			fmt.Printf("cl=%x: [BEGIN] chat conn request\n", c.iddev)
			if _, err = c.conn.C.Read(root[:]); err != nil {
				fmt.Printf("cl=%x error reading rt: %v\n", c.iddev, err)
				continue
			}
			if utils.Contains(root, c.rooms) {
				fmt.Printf("cl=%x: already connected to rt=%x\n", c.iddev, root)
				continue
			}
			connect(c, root)
			fmt.Printf("cl=%x: [DONE] chat conn request\n", c.iddev)

		case CHAT_DISC_REQ:
			fmt.Printf("cl=%x: [BEGIN] chat disc request\n", c.iddev)
			if _, err = c.conn.C.Read(root[:]); err != nil {
				fmt.Printf("cl=%x error reading root: %v\n", c.iddev, err)
				continue
			}
			if !utils.Contains(root, c.rooms) {
				fmt.Printf("cl=%x: already disc to rt=%x\n", c.iddev, root)
				continue
			}
			disconnect(c, root)
			fmt.Printf("cl=%x: [DONE] chat disc request\n", c.iddev)

		case SCROLL_REQUEST:
			fmt.Printf("cl=%x: [BEGIN] scroll request\n", c.iddev)
			err = utils.ReadBin(c.conn.C, &isBefore, root[:], &ts, &limit, &isSnips)
			if err != nil {
				fmt.Printf("cl=%x: error reading rest of request\n", c.iddev)
				continue
			}
			fmt.Printf("cl=%x: before=%v, root=%x, ts=%d, limit=%d\n",
				c.iddev, isBefore, root, ts, limit)

			go HandleChatScroll3(c.conn, isBefore, root, ts, limit, isSnips)
			fmt.Printf("cl=%x: [DONE] scroll request\n", c.iddev)

		case BOOSTS_REQ:
			fmt.Printf("cl=%x: [BEGIN] boost req\n", c.iddev)
			copy(nodeId[:], msgBuf[:])
			// nodeId := msgBuf[:id_utils.RAW_NODE_ID_LEN]
			err = utils.ReadBin(c.conn.C, nodeId[:], &ts, &limit)
			if err != nil {
				fmt.Printf("cl=%x: err reading request: %v\n", c.iddev, err)
				continue
			}
			go HandleBoostScroll3(c.conn, nodeId, ts, limit)
			fmt.Printf("cl=%x: [DONE] boost req\n", c.iddev)

		case CHAT_EVENT:
			mbuf := msgBuf[:reqLen]
			fmt.Printf("cl=%x: [BEGIN] chat event\n", c.iddev)
			if _, err = c.conn.C.Read(mbuf); err != nil {
				fmt.Printf("cl=%x: error reading msg: %v\n", c.iddev, err)
				continue
			}
			cid := flatgen.GetRootAsMessageEvent(mbuf, 0).ChatId(nil)
			id_utils.WriteMsgId(preBuf, cid)
			preBuf.Reset()

			rt := cid.Root(nil)
			var confirmed bool
			if confirmed = rt.Confirmed(); !confirmed {
				rt.MutateConfirmed(true)
			}

			id_utils.WriteRoot(rootBuf, rt)
			rootBuf.Reset()

			fmt.Printf("cl=%x: rt=%X\n", c.iddev, root)
			msgreq := &msg2{mbuf: mbuf, root: root, pre: pre, res: res}
			handleChatEvent(c, msgreq, confirmed)

			// fun special case
			if !confirmed {
				rootT, pushed, err := GetOrPushRoot(rt.UnPack())
				if pushed {
					connect(c, root)
					l := uint16(2 * id_utils.RAW_MSG_ID_LEN)
					c.conn.Locked(func(w io.Writer) {
						utils.WriteBin(w, MESSAGE_SENT, l, pre[:])
						id_utils.WriteMsgId(w, cid)
					})
				} else if err != nil {
					l := uint16(id_utils.RAW_MSG_ID_LEN)
					c.conn.WriteBin(ROOT_ERROR, l, pre[:])
				} else if rootT.ChatPlace != rt.ChatPlace() {
					l := uint16(id_utils.RAW_ROOT_ID_LEN +
						id_utils.RAW_MSG_ID_LEN)
					c.conn.Locked(func(w io.Writer) {
						utils.WriteBin(w, NEW_ROOT_PLACE, l)
						id_utils.WriteRootT(w, rootT)
						utils.WriteBin(w, pre[:])
					})
				} else {
					root_ := id_utils.RawRoot2(rootT)
					msg := &msg2{mbuf: mbuf, root: root_, pre: pre, res: res}
					rt := flatgen.GetRootAsMessageEvent(mbuf, 0).
						ChatId(nil).Root(nil)
					rt.MutateTimestamp(rootT.Timestamp)
					rt.MutateU32(rootT.U32)
					connect(c, root_)
					handleChatEvent(c, msg, true)
				}
			}

			fmt.Printf("cl=%x: [DONE] chat event\n", c.iddev)

		case NOTIFICATIONS:
			ntfbuf := msgBuf[:reqLen]
			if _, err := c.conn.C.Read(ntfbuf); err != nil {
				fmt.Printf("cl=%x: error reading ntf: %v\n", c.iddev, err)
				continue
			}
			fmt.Printf("cl=%x: [BEGIN] notification\n", c.iddev)
			ntfs := flatgen.GetRootAsNotifications(ntfbuf, 0)
			SendNotifications(ntfs)
			fmt.Printf("cl=%x: [DONE] notification\n", c.iddev)
		}
	}
}
