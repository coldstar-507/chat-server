package db

import (
	"context"
	"errors"
	"os"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/coldstar-507/utils/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/storage"
	"google.golang.org/api/option"

	"github.com/gocql/gocql"
)

var (
	store_path string

	err      error
	Messager *messaging.Client

	FirebaseApp *firebase.App

	Mongo *mongo.Client
	dbOne *mongo.Database
	Nodes *mongo.Collection

	// LV         *leveldb.DB
	// store      storage.Storage
)

func InitFirebaseApp() {
	servAcc := os.Getenv("FIREBASE_CONFIG")
	opt := option.WithCredentialsFile(servAcc)
	app, err := firebase.NewApp(context.Background(), nil, opt)
	utils.Fatal(err, "InitFirebaseMessager error creating firebase app")
	FirebaseApp = app
}

func InitFirebaseMessager() {
	Messager, err = FirebaseApp.Messaging(context.Background())
	utils.Fatal(err, "InitFirebaseMessager error creating firebase messager")
}

var Scy *gocql.Session

func createBoostTable() error {
	err := Scy.Query(`CREATE KEYSPACE IF NOT EXISTS db_one
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`).Exec()
	if err != nil {
		return err
	}

	// err = Scy.Query(`ALTER KEYSPACE db_one
	// WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`).Exec()
	// if err != nil {
	// 	return err
	// }

	// err = Scy.Query("DROP TABLE db_one.boosts").Exec()
	// if err != nil {
	// 	return err
	// }

	err0 := Scy.Query(`CREATE TABLE IF NOT EXISTS db_one.pushes (
                           node_id BLOB,
                           dev     INT,
                           ts      BIGINT,
                           nonce   INT,
                           type    TINYINT,
                           payload BLOB,
                           PRIMARY KEY ((node_id, dev), ts, nonce))`).Exec()

	err1 := Scy.Query(`CREATE TABLE IF NOT EXISTS db_one.boosts (
                           node_id BLOB,
                           ts      BIGINT,
                           nonce   INT,
                           booster BLOB,
                           PRIMARY KEY (node_id, ts, nonce))`).Exec()

	err2 := Scy.Query(`CREATE TABLE IF NOT EXISTS db_one.messages (
                           root    BLOB,
                           ts      BIGINT,
                           nonce   INT,
                           msg     BLOB,
                           PRIMARY KEY (root, ts, nonce))`).Exec()

	err3 := Scy.Query(`CREATE TABLE IF NOT EXISTS db_one.snips (
                           root    BLOB,
                           ts      BIGINT,
                           nonce   INT,
                           msg     BLOB,
                           PRIMARY KEY (root, ts, nonce))`).Exec()

	return errors.Join(err0, err1, err2, err3)
}

func InitScylla() {
	var cluster = gocql.NewCluster("localhost:9042")
	var err error
	Scy, err = cluster.CreateSession()
	utils.Must(err)
	err = createBoostTable()
	utils.Must(err)
}

func ShutdownScylla() {
	Scy.Close()
}

func InitMongo() {
	// TODO set in env
	uri := "mongodb://localhost:27100,localhost:27200"
	opt := options.Client().
		SetReadPreference(readpref.Primary()).
		ApplyURI(uri)

	Mongo, err = mongo.Connect(context.TODO(), opt)
	utils.Must(err)
	dbOne = Mongo.Database("one")
	Nodes = dbOne.Collection("nodes")

}

func ShutdownMongo() {
	if err := Mongo.Disconnect(context.TODO()); err != nil {
		panic(err)
	}
}

// func InitLevelDb() {
// 	store_path = os.Getenv("DATA_PATH")
// 	if len(store_path) == 0 {
// 		panic("DATA_PATH is not properly configured in ENV")
// 	}

// 	store, err = storage.OpenFile(store_path, false)
// 	utils.Fatal(err, "InitLevelDb error opening store")
// 	LV, err = leveldb.Open(store, nil)
// 	utils.Fatal(err, "InitLevelDb error opening leveldb")
// }

// func ShutDownLevelDb() {
// 	utils.NonFatal(LV.Close(), "ShutDownLevelDb error closing leveldb")
// 	utils.NonFatal(store.Close(), "ShutDownLevelDb error closing store")
// }
