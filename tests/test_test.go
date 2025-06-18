package test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/chat-server/internal/handlers"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils2"
)

type bw struct {
	buf *bytes.Buffer
}

func makeBw() *bw {
	return &bw{buf: new(bytes.Buffer)}
}

func (b *bw) WriteBin(bin ...any) error {
	return utils2.WriteBin(b.buf, bin...)
}

func TestMain(m *testing.M) {
	db.InitScylla()
	defer db.ShutdownScylla()
	code := m.Run()
	os.Exit(code)
}

const (
	root1 = "010500000191a66ff1498666d1ca05000001925000d51021e8c6c9000001925083c76d100001"
	// root2 = "0105000001930eb6bf7643f657bb05000000000000000000000000000001930eb6c13f100001"
)

var (
	r1 = utils2.RootFromString(root1)
	// r2 = utils2.RootFromString(root2)

	n1 = utils2.NodeIdFromString("0500000191ab9012bb6a76a855")
	// n2 = utils2.NodeIdFromString("0500000191d7a640e00b756fcc")
	// n3 = utils2.NodeIdFromString("0500000191d7ca97d10f32d204")
)

func TestScrollChatAfter(t *testing.T) {
	bw := makeBw()
	rawRoot1 := utils2.RawRoot(r1)
	var testTs int64 = 1734639057785
	handlers.HandleChatScrollSync(bw, false, rawRoot1, testTs, 0xffff, false)

	var (
		k byte
		l uint16
		v []byte
	)

	fmt.Printf("HandleChatScrollAfter %d:\n", testTs)
	for {
		if err := utils2.ReadBin(bw.buf, &k, &l); err != nil {
			break
		}
		if cap(v) < int(l) {
			v = make([]byte, l)
		} else {
			v = v[:l]
		}
		utils2.ReadBin(bw.buf, v)
		fmt.Printf("k: %X\nl: %d\nv: %X\n", k, l, v)
		switch k {
		case handlers.CHAT_EVENT:
			me := flatgen.GetRootAsMessageEvent(v, 0)
			fmt.Printf("msgTs : %d\nmsgTxt: %s\n", me.Timestamp(), me.Txt())
		case handlers.CHAT_SCROLL_DONE:
			fmt.Printf("scroll done, raw payload: %X\n", v)
		default:
			t.Errorf("invalid chat server event type: %X\n", k)
		}
	}
}

func TestScrollChatBefore(t *testing.T) {
	bw := makeBw()
	rawRoot1 := utils2.RawRoot(r1)
	var testTs int64 = 1734639057785
	handlers.HandleChatScrollSync(bw, true, rawRoot1, testTs, 0xffff, false)

	var (
		k byte
		l uint16
		v []byte
	)

	fmt.Printf("HandleChatScrollBefore %d:\n", testTs)
	for {
		if err := utils2.ReadBin(bw.buf, &k, &l); err != nil {
			break
		}
		if cap(v) < int(l) {
			v = make([]byte, l)
		} else {
			v = v[:l]
		}
		utils2.ReadBin(bw.buf, v)
		fmt.Printf("k: %X\nl: %d\nv: %X\n", k, l, v)
		switch k {
		case handlers.CHAT_EVENT:
			me := flatgen.GetRootAsMessageEvent(v, 0)
			fmt.Printf("msgTs : %d\nmsgTxt: %s\n", me.Timestamp(), me.Txt())
		case handlers.CHAT_SCROLL_DONE:
			fmt.Printf("scroll done, raw payload: %X\n", v)
		default:
			t.Errorf("invalid chat server event type: %X\n", k)
		}
	}
}

func TestAfterDevice(t *testing.T) {
	buf := new(bytes.Buffer)
	var d1 uint32 = 3380306940
	rawN1 := utils2.RawNodeId(n1)
	var testTs int64 = 1734577797572
	handlers.ReadPushes(buf, rawN1, d1, testTs)

	var (
		k  byte
		ts int64
		l  uint16
		p  []byte
	)

	for {
		if err := utils2.ReadBin(buf, &k, &ts, &l); err != nil {
			break
		}
		if cap(p) < int(l) {
			p = make([]byte, l)
		} else {
			p = p[:l]
		}
		utils2.ReadBin(buf, p)
		fmt.Printf("k : %X\nts: %d\nl : %d\np : %X\n", k, ts, l, p)
		if k == 0x01 {
			fmt.Printf("text: %s\ntime: %d\n", string(p), ts)
		}
	}
}
