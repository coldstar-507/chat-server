package test

import (
	"log"
	"os"
	"testing"

	tools "github.com/coldstar-507/chat-server/internal"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestMain(m *testing.M) {
	os.Chdir("../")
	db.InitLevelDb()
	defer db.ShutDownLevelDb()
	code := m.Run()
	os.Exit(code)
}

func TestPrefixExtractor(t *testing.T) {
	ref := []byte("group0-1718911865490-4dc97ca7592e3850-us-c")
	p_ := tools.ExtractNthIdPrefix(ref, -1)
	p0 := tools.ExtractNthIdPrefix(ref, 0)
	p1 := tools.ExtractNthIdPrefix(ref, 1)
	p2 := tools.ExtractNthIdPrefix(ref, 2)
	p3 := tools.ExtractNthIdPrefix(ref, 3)
	p4 := tools.ExtractNthIdPrefix(ref, 4)
	p5 := tools.ExtractNthIdPrefix(ref, 5)
	p6 := tools.ExtractNthIdPrefix(ref, 6)
	log.Println("p-1", string(p_))
	log.Println("p0", string(p0))
	log.Println("p1", string(p1))
	log.Println("p2", string(p2))
	log.Println("p3", string(p3))
	log.Println("p4", string(p4))
	log.Println("p5", string(p5))
	log.Println("p6", string(p6))

	if string(p0) != "group0" {
		t.Error(string(p0), "!=", "group0")
	} else if string(p1) != "group0" {
		t.Error(string(p1), "!=", "group0")
	} else if string(p2) != "group0-1718911865490" {
		t.Error(string(p2), "!=", "group0-1718911865490")
	} else if string(p3) != "group0-1718911865490-4dc97ca7592e3850" {
		t.Error(string(p3), "!=", "group0-1718911865490-4dc97ca7592e3850")
	} else if string(p4) != "group0-1718911865490-4dc97ca7592e3850-us" {
		t.Error(string(p4), "!=", "group0-1718911865490-4dc97ca7592e3850-us")
	} else if string(p5) != "group0-1718911865490-4dc97ca7592e3850-us" {
		t.Error(string(p5), "!=", "group0-1718911865490-4dc97ca7592e3850-us")
	} else if string(p6) != "group0-1718911865490-4dc97ca7592e3850-us" {
		t.Error(string(p6), "!=", "group0-1718911865490-4dc97ca7592e3850-us")
	}

}

func TestAll(t *testing.T) {
	it := db.LV.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		log.Println(string(it.Key()))
	}
}

func TestAfterId(t *testing.T) {
	ref := []byte("group0-1718911865490-4dc97ca7592e3850-us-c")
	prefix := tools.ExtractIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("AFTER")
	log.Println(string(ref))
	log.Println("=====")

	for ok := it.Seek(ref); ok; ok = it.Next() {
		log.Println(string(it.Key()))
	}
}

func TestBeforeId(t *testing.T) {
	ref := []byte("group0-1718911865490-4dc97ca7592e3850-us-c")
	prefix := tools.ExtractIdPrefix(ref)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("BEFORE")
	log.Println(string(ref))
	log.Println("======")

	it.Seek(ref)
	for it.Prev() {
		log.Println(string(it.Key()))
	}
}

func TestAfterDevice(t *testing.T) {
	ref := []byte("user0-dev0-1719433729807-7d84ffcb9bfd6a58-us-p")
	prefix := tools.ExtractNthIdPrefix(ref, 2)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("AFTER")
	log.Println(string(ref))
	log.Println("=====")

	for ok := it.Seek(ref); ok; ok = it.Next() {
		log.Println(string(it.Key()))
	}	
}

func TestBeforeDevice(t *testing.T) {
	ref := []byte("user0-dev0-1719433729807-7d84ffcb9bfd6a58-us-p")
	prefix := tools.ExtractNthIdPrefix(ref, 2)
	it := db.LV.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()

	log.Println("BEFORE")
	log.Println(string(ref))
	log.Println("======")

	it.Seek(ref)
	for it.Prev() {
		log.Println(string(it.Key()))
	}
}
