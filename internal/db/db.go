package db

import (
	"context"
	"os"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/coldstar-507/utils/utils"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"google.golang.org/api/option"
)

var (
	store_path string
	LV         *leveldb.DB
	err        error
	Messager   *messaging.Client
	store      storage.Storage
)

func InitFirebaseMessager() {
	servAcc := os.Getenv("FIREBASE_CONFIG")
	opt := option.WithCredentialsFile(servAcc)
	app, err := firebase.NewApp(context.Background(), nil, opt)
	utils.Fatal(err, "InitFirebaseMessager error creating firebase app")
	Messager, err = app.Messaging(context.Background())
	utils.Fatal(err, "InitFirebaseMessager error creating firebase messager")
}

func InitLevelDb() {
	store_path = os.Getenv("DATA_PATH")
	if len(store_path) == 0 {
		panic("DATA_PATH is not properly configured in ENV")
	}

	store, err = storage.OpenFile(store_path, false)
	utils.Fatal(err, "InitLevelDb error opening store")
	LV, err = leveldb.Open(store, nil)
	utils.Fatal(err, "InitLevelDb error opening leveldb")
}

func ShutDownLevelDb() {
	utils.NonFatal(LV.Close(), "ShutDownLevelDb error closing leveldb")
	utils.NonFatal(store.Close(), "ShutDownLevelDb error closing store")
}

// var S *gocql.Session
// var S_ gocqlx.Session
// var MET = table.New(message_events_table)

// var message_events_table = table.Metadata{
// 	Name:    "message_events",
// 	Columns: []string{"timeId", "type", "senderId", "root", "timestamp", "forwardedFrom", "paymentId", "nodes", "replies", "txt", "mediaId", "tempMedia", "tempPayment", "messageId", "reactors", "reactionsId"},
// 	PartKey: []string{"root"},
// 	SortKey: []string{"timeId"},
// }

// var medias_metadata_table = table.Metadata{
// 	Name:    "medias_metadata",
// 	Columns: []string{"timeId", "ownerId", "timestamp", "mime", "isReversed", "isEncrypted", "isPaidToView", "isPaidToOwn", "isLocked", "isSaved", "temp"},
// 	PartKey: []string{"timeId"},
// 	SortKey: []string{"timeId"},
// }

// var createMessages = `
// create table if not exists message_events (
//   root string, time_id string, type tinyint data blob,
//   primary key (root, time_id))
// `

// var createChats = `
// create table if not exists chats (
//   root string, time_id string, type string, data blob,
//   primary key (root, time_id))
// `

// var createReactions = `
// create table if not exists reactions (
//   chat_time_id string, time_id string, data blob,
//   primary key (chat_time_id, time_id))
// `
// var createMedias = `

//   time_id string, data blob,
//   primary key time_id)
// `

// func Init(ctx context.Context) {
// 	cluster := gocql.NewCluster()
// 	S, err = cluster.CreateSession()
// 	// utils.Fatal(err, "initilizing session")
// 	S_, err = gocqlx.WrapSession(cluster.CreateSession())

// 	utils.Fatal(S.Query(createMessages, nil).Exec(), "creating messages")
// 	utils.Fatal(S.Query(createReactions, nil).Exec(), "creating reactions")
// 	utils.Fatal(S.Query(createMedias, nil).Exec(), "creating medias")

// 	servAcc := os.Getenv("FIREBASE_CONFIG")
// 	opt := option.WithCredentialsFile(servAcc)
// 	app, err := firebase.NewApp(ctx, nil, opt)
// 	utils.Fatal(err, "error creating firebase app")
// 	Messager, err = app.Messaging(ctx)
// 	utils.Fatal(err, "error creating firebase messager")
// }

// func Shutdown() {
// 	S.Close()
// }
