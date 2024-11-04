package test

import (
	"bytes"
	"encoding/hex"
	"log"
	"os"
	"testing"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestMain(m *testing.M) {
	os.Chdir("../")
	db.InitLevelDb()
	defer db.ShutDownLevelDb()
	code := m.Run()
	os.Exit(code)
}

func TestPrefixs(t *testing.T) {
	zeroRoot := &flatgen.NodeIdT{
		Timestamp: 0,
		U32:       0,
		Prefix:    utils.KIND_NODE,
	}

	singleRootT := flatgen.RootT{
		Primary: &flatgen.NodeIdT{
			Timestamp: utils.MakeTimestamp(),
			U32:       utils.RandU32(),
			Prefix:    utils.KIND_NODE,
		},
		Secondary: zeroRoot,
	}

	dualRootT := flatgen.RootT{
		Primary: &flatgen.NodeIdT{
			Timestamp: utils.MakeTimestamp(),
			U32:       utils.RandU32(),
			Prefix:    utils.KIND_NODE,
		},
		Secondary: &flatgen.NodeIdT{
			Timestamp: utils.MakeTimestamp(),
			U32:       utils.RandU32(),
			Prefix:    utils.KIND_NODE,
		},
	}

	msgIdT0 := flatgen.MessageIdT{
		Prefix:    utils.KIND_MESSAGE,
		Root:      &singleRootT,
		Timestamp: utils.MakeTimestamp(),
		U32:       utils.RandU32(),
		Suffix:    utils.Chat,
	}

	msgIdT1 := flatgen.MessageIdT{
		Prefix:    utils.KIND_MESSAGE,
		Root:      &dualRootT,
		Timestamp: utils.MakeTimestamp(),
		U32:       utils.RandU32(),
		Suffix:    utils.Chat,
	}

	pushIdT := flatgen.PushIdT{
		U32:       utils.RandU32(),
		Timestamp: utils.MakeTimestamp(),
		Device:    utils.RandU32(),
		NodeId:    singleRootT.Primary,
		Prefix:    utils.KIND_PUSH,
	}

	b1 := flatbuffers.NewBuilder(60)
	b1.Finish(singleRootT.Pack(b1))
	root0 := flatgen.GetRootAsRoot(b1.FinishedBytes(), 0)

	b2 := flatbuffers.NewBuilder(60)
	b2.Finish(dualRootT.Pack(b2))
	root1 := flatgen.GetRootAsRoot(b2.FinishedBytes(), 0)

	b3 := flatbuffers.NewBuilder(60)
	b3.Finish(msgIdT0.Pack(b3))
	msgId0 := flatgen.GetRootAsMessageId(b3.FinishedBytes(), 0)

	b4 := flatbuffers.NewBuilder(60)
	b4.Finish(msgIdT1.Pack(b4))
	msgId1 := flatgen.GetRootAsMessageId(b4.FinishedBytes(), 0)

	b5 := flatbuffers.NewBuilder(60)
	b5.Finish(pushIdT.Pack(b5))
	pushId := flatgen.GetRootAsPushId(b5.FinishedBytes(), 0)

	rawRoot0 := utils.MakeRawRoot(root0)
	if len(rawRoot0) != utils.RAW_ROOT_ID_LEN {
		t.Errorf("len rawRoot0=%d\nexpected=%d\nvalue=%v\n",
			len(rawRoot0), utils.RAW_ROOT_ID_LEN, rawRoot0)
	}

	rawRoot1 := utils.MakeRawRoot(root1)
	if len(rawRoot1) != utils.RAW_ROOT_ID_LEN {
		t.Errorf("len rawRoot1=%d\nexpected=%d\nvalue=%v\n",
			len(rawRoot1), utils.RAW_ROOT_ID_LEN, rawRoot1)
	}

	rawMsgId0 := utils.MakeRawMsgId(msgId0)
	if len(rawMsgId0) != utils.RAW_MSG_ID_LEN {
		t.Errorf("len rawMsgId0=%d\nexpected=%d\nvalue=%v\n",
			len(rawMsgId0), utils.RAW_MSG_ID_LEN, rawMsgId0)
	}

	rawMsgId1 := utils.MakeRawMsgId(msgId1)
	if len(rawMsgId1) != utils.RAW_MSG_ID_LEN {
		t.Errorf("len rawMsgId1=%d\nexpected=%d\nvalue=%v\n",
			len(rawMsgId1), utils.RAW_MSG_ID_LEN, rawMsgId1)
	}

	rawPushId := utils.MakeRawPushId(pushId)
	if len(rawPushId) != utils.RAW_PUSH_ID_LEN {
		t.Errorf("len rawPushId=%d\nexpected=%d\nvalue=%v\n",
			len(rawPushId), utils.RAW_PUSH_ID_LEN, rawPushId)
	}

	msgIdPrefix0 := append([]byte{0x00}, rawRoot0...)
	if prefix := utils.MsgIdPrefix(rawMsgId0); !bytes.Equal(prefix, msgIdPrefix0) {
		t.Errorf(`
expected rawMsgId0 prefix len = %d
got                           = %d
expected value = %x
got            = %x`, 1+utils.RAW_ROOT_ID_LEN, len(prefix), msgIdPrefix0, prefix)

	}

	msgIdPrefix1 := append([]byte{0x00}, rawRoot1...)
	if prefix := utils.MsgIdPrefix(rawMsgId1); !bytes.Equal(prefix, msgIdPrefix1) {
		t.Errorf(`
expected rawMsgId1 prefix len = %d
got                           = %d
expected value = %x
got            = %x`, 1+utils.RAW_ROOT_ID_LEN, len(prefix), msgIdPrefix1, prefix)
	}

	if prefix := utils.PushIdPrefix(rawPushId); !bytes.Equal(
		prefix, rawPushId[:utils.RAW_PUSH_ID_PREFIX_LEN]) {
		t.Errorf(`
expected pushId prefix len = %d
got                        = %d
expected value = %x
got            = %x`, utils.RAW_PUSH_ID_PREFIX_LEN, len(prefix),
			rawPushId[:utils.RAW_NODE_ID_LEN+4], prefix)
	}

}

func TestAfterId(t *testing.T) {
	id := "00000190d7f3ab18cdcdcdcd0c00000190cc05595f10000500000190e00344b72a45879401"
	ref, _ := hex.DecodeString(id)
	prefix := utils.MsgIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("AFTER")
	log.Println(id)
	log.Printf("prefix: %x\n", prefix)
	log.Println("=====")

	for ok := it.Seek(ref); ok; ok = it.Next() {
		log.Printf("found key: %x\n", it.Key())
	}
}

func TestBeforeId(t *testing.T) {
	id := "00000190d7f3ab18cdcdcdcd0c00000190cc05595f10000500000190e00344b72a45879401"
	ref, _ := hex.DecodeString(id)
	prefix := utils.MsgIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("BEFORE")
	log.Println(id)
	log.Printf("prefix: %x\n", prefix)
	log.Println("======")

	it.Seek(ref)
	for it.Prev() {
		log.Printf("found key: %x\n", it.Key())
	}
}

func TestAfterDevice(t *testing.T) {
	hexRef := "00000190d7f3ab18cdcdcdcd0cabababab00000190dbdb559d000000000b"
	ref, _ := hex.DecodeString(hexRef)
	prefix := utils.PushIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("AFTER")
	log.Println(hexRef)
	log.Printf("prefix: %x\n", prefix)
	log.Println("=====")

	for ok := it.Seek(ref); ok; ok = it.Next() {
		log.Printf("found key: %x\n", it.Key())
	}
}

func TestBeforeDevice(t *testing.T) {
	hexRef := "00000190d7f3ab18cdcdcdcd0cabababab00000190dbdb559d000000000b"
	ref, _ := hex.DecodeString(hexRef)
	prefix := utils.PushIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("BEFORE")
	log.Println(hexRef)
	log.Printf("prefix: %x\n", prefix)
	log.Println("======")

	it.Seek(ref)
	for it.Prev() {
		log.Printf("found key: %x\n", it.Key())
	}
}
