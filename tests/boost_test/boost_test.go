package test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/chat-server/internal/handlers"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils2"
)

func TestMain(m *testing.M) {
	// os.Chdir("../")
	// db.InitLevelDb()
	// defer db.ShutDownLevelDb()

	db.InitScylla()
	defer db.ShutdownScylla()
	code := m.Run()
	os.Exit(code)
}

type Binwriter struct {
	buf *bytes.Buffer
}

func (cc *Binwriter) WriteBin(vals ...any) error {
	return utils2.WriteBin(cc.buf, vals...)
}

func TestBoostScroll(t *testing.T) {
	// nodeId, _ := hex.DecodeString("05000001931C2BFF6E14CB9DAA")
	madaraId, _ := hex.DecodeString("050000019321563A0FE412A126")
	// nodeId, _ := hex.DecodeString("05000001931C3483F3A17ECF2B")
	var ts int64 = 1733419823721 - 1
	// var ts uint64 = 1733196890732

	bw := &Binwriter{buf: new(bytes.Buffer)}

	handlers.HandleBoostScroll(bw, madaraId, ts, 100)
	var (
		n = 0
		k byte
		l uint16
		p []byte
	)
	for {
		if err := utils2.ReadBin(bw.buf, &k, &l); err != nil {
			fmt.Println("Read error:", err, "breaking")
			break
		}
		n++
		if cap(p) < int(l) {
			p = make([]byte, l)
		}
		utils2.ReadBin(bw.buf, p[:l])
		fmt.Printf("t=%d, l=%d\n", k, l)
		fb := flatgen.GetRootAsBooster(p, 0)
		fmt.Printf(`
s1       : %X
sats     : %d
utxoIx   : %d
ts       : %d
`, fb.SecretBytes(), fb.Sats(), fb.UtxoIx(), fb.Timestamp())

		fmt.Print("interests: ")
		for i := range fb.InterestsLength() {
			fmt.Print(string(fb.Interests(i)), " ")
		}
		fmt.Print("\n")

		msgId := fb.MsgId(nil)
		root := utils2.MakeRawRoot(msgId.Root(nil))
		// 01050000000000000000000000000500000000000000000000000000FFFFFFFFFFFFFFFFFF
		// 01050000000000000000000000000500000000000000000000000000FFFFFFFFFFFFFFFFFF

		fmt.Printf(`Searching Msg for:
root  : %X
ts    : %d
nonce : %d
`, root, msgId.Timestamp(), msgId.U32())

		if msg, err := handlers.GetMsg(root, msgId.Timestamp(), msgId.U32()); err != nil {
			fmt.Println("Error getting msg:", err)
		} else {
			m := flatgen.GetRootAsMessageEvent(msg, 0).UnPack()
			fmt.Printf(`
senderId  : %s
senderTag : %s
text      : %s
nodes     : %v
ts        : %d
`, m.SenderId, m.SenderTag, m.Txt, m.Nodes, m.Timestamp)
		}

	}
	fmt.Println("Found", n, "boosts")
	fmt.Println("DONE")
}
