package handlers

import (
	"fmt"
	"log"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/utils2"
)

func If[T any](condition bool, If, Else T) T {
	if condition {
		return If
	}
	return Else
}

func HandleBoostScroll2(c utils2.Binw, nodeId []byte, ts int64, dev uint32) {
	stmt0 := `UPDATE booster FROM db_one.boosts
                  SET device = ?
                  WHERE nodeId = ? AND ts > ?
                  IF device == NULL`
	q0 := db.Scy.Query(stmt0, nodeId, ts, dev)
	fmt.Println(q0.String())
	defer q0.Release()
	if err := q0.Exec(); err != nil {
		log.Println("HandleBoostScroll2: error marking boosters:", err)
		return
	}

	stmt1 := `SELECT booster FROM db_one.boosts
                  WHERE node_id = ? AND ts > ? AND device = ?`
	q1 := db.Scy.Query(stmt1, nodeId, ts, dev)
	fmt.Println(q1.String())
	defer q1.Release()
	iter := q1.Iter()
	defer iter.Close()

	var booster []byte
	for iter.Scan(&booster) {
		c.WriteBin(BOOST_EVENT, uint16(len(booster)), booster)
	}
	log.Println("handleBoostScroll2: done")
}

func HandleBoostScroll(c utils2.Binw, nodeId []byte, ts int64, limit uint16) {
	stmt := `SELECT booster FROM db_one.boosts
                 WHERE node_id = ? and ts > ? LIMIT ?`
	q := db.Scy.Query(stmt, nodeId, ts, limit+1)
	log.Println(q.String())
	defer q.Release()
	iter := q.Iter()
	defer iter.Close()

	var booster []byte
	for limit > 0 && iter.Scan(&booster) {
		log.Printf("yielding boost: t=%d, l=%d\n", BOOST_EVENT, uint16(len(booster)))
		err := c.WriteBin(BOOST_EVENT, uint16(len(booster)), booster)
		if err != nil {
			log.Println("ERROR writing BIN:", err)
		}
		limit--
	}
	isMore := iter.Scan(&booster)
	c.WriteBin(c, BOOST_SCROLL_DONE, uint16(1), isMore)
	log.Println("handleBoostScroll: done")
}

func HandleBoostScroll3(c utils2.Binw, nodeId utils2.NodeId, ts int64, limit uint16) {
	stmt := `SELECT booster FROM db_one.boosts
                 WHERE node_id = ? and ts > ? LIMIT ?`
	q := db.Scy.Query(stmt, nodeId[:], ts, limit+1)
	log.Println(q.String())
	defer q.Release()
	iter := q.Iter()
	defer iter.Close()

	var booster []byte
	for limit > 0 && iter.Scan(&booster) {
		log.Printf("yielding boost: t=%d, l=%d\n", BOOST_EVENT, uint16(len(booster)))
		err := c.WriteBin(BOOST_EVENT, uint16(len(booster)), booster)
		if err != nil {
			log.Println("ERROR writing BIN:", err)
		}
		limit--
	}
	isMore := iter.Scan(&booster)
	c.WriteBin(c, BOOST_SCROLL_DONE, uint16(1), isMore)
	log.Println("handleBoostScroll: done")
}

func HandleChatScrollSync(c utils2.Binw, before bool, root []byte, ts int64,
	limit uint16, snips bool) {

	stmt := fmt.Sprintf(
		`SELECT msg FROM db_one.%s
                 WHERE root = ? AND ts %s ?
                 ORDER BY ts %s LIMIT ?`, If(snips, "snips", "messages"),
		If(before, "<", ">"), If(before, "DESC", "ASC"))

	q := db.Scy.Query(stmt, root, ts, limit+1) // fetch one more to check if hasMore
	fmt.Println(q.String())
	defer q.Release()
	it := q.Iter()
	var msg []byte
	for limit > 0 && it.Scan(&msg) {
		log.Println("found value")
		c.WriteBin(CHAT_EVENT_FROM_SCROLL, uint16(len(msg)), msg)
		log.Println("wrote value")
		limit--
	}

	fmt.Print("Check if more")
	hasMore := it.Scan(&msg)
	fmt.Printf("hasmore? %v\n", hasMore)

	fmt.Println("writing response")
	c.WriteBin(CHAT_SCROLL_DONE, uint16(1+1+1+len(root)), hasMore, before, snips, root)
	fmt.Println("done writing response")
}

func HandleChatScroll3(c utils2.Binw, before bool, root utils2.Root, ts int64,
	limit uint16, snips bool) {

	stmt := fmt.Sprintf(
		`SELECT msg FROM db_one.%s
                 WHERE root = ? AND ts %s ?
                 ORDER BY ts %s LIMIT ?`, If(snips, "snips", "messages"),
		If(before, "<", ">"), If(before, "DESC", "ASC"))

	q := db.Scy.Query(stmt, root[:], ts, limit+1) // fetch one more to check if hasMore
	defer q.Release()
	it := q.Iter()
	var msg []byte
	for limit > 0 && it.Scan(&msg) {
		log.Println("found value")
		c.WriteBin(CHAT_EVENT_FROM_SCROLL, uint16(1+len(msg)), before, msg)
		log.Println("wrote value")
		limit--
	}

	fmt.Println("Check if more")
	hasMore := it.Scan(&msg)
	fmt.Printf("hasmore? %v\n", hasMore)

	fmt.Println("writing response")
	c.WriteBin(CHAT_SCROLL_DONE, uint16(1+1+1+len(root)), hasMore, before, snips, root[:])
	fmt.Println("done writing response")
}
