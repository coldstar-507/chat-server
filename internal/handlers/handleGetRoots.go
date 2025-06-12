package handlers

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	// "time"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils/id_utils"
	"github.com/coldstar-507/utils/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func readRoots(ctx context.Context, hexId string) ([]*flatgen.RootT, error) {
	filter := bson.M{"_id": hexId}
	r := db.Nodes.FindOne(ctx, filter)
	var res struct {
		Roots []string `bson:"roots"`
	}

	if err := r.Decode(&res); err != nil {
		return nil, err
	}

	roots := utils.Map(res.Roots, func(r string) *flatgen.RootT {
		return id_utils.ParseRoot(hex.NewDecoder(strings.NewReader(r)))
	})

	return roots, nil
}

func AddRoots(ctx context.Context, root string, ids ...string) error {
	update := bson.M{
		"$addToSet":    bson.M{"roots": root},
		"$currentDate": bson.M{"lastUpdate": true},
	}
	filter := bson.M{"_id": bson.M{"$in": ids}}
	r, err := db.Nodes.UpdateMany(ctx, filter, update)
	fmt.Printf(`AddRoots:
MatchedCount : %d
ModifiedCount: %d
UpsertedCount: %d
UpsertedId   : %v
`, r.MatchedCount, r.ModifiedCount, r.UpsertedCount, r.UpsertedID)

	fmt.Printf("r: %v\n", r)
	return err
}

func AddNewRoot(ctx context.Context, id, root string) error {
	update := bson.M{
		"$addToSet":    bson.M{"roots": root},
		"$currentDate": bson.M{"lastUpdate": true},
	}
	_, err := db.Nodes.UpdateByID(ctx, id, update)
	return err

}

func GetOrPushRootFromGroup(potNewRoot *flatgen.RootT) (*flatgen.RootT, bool, error) {
	var pushedRoot bool
	groupId := id_utils.HexNodeId(potNewRoot.Primary)
	err := db.Mongo.UseSession(context.Background(), func(sc mongo.SessionContext) error {
		err := sc.StartTransaction()
		if err != nil {
			return err
		}

		roots, err := readRoots(sc, groupId)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}

		// no need to check for homological, all roots of group are homological
		if len(roots) == 0 {
			err = AddNewRoot(sc, groupId, id_utils.HexRoot(potNewRoot))
			if err != nil {
				return err
			}
			pushedRoot = true
		} else {
			mur := id_utils.MostUpToDateRoot(roots)
			if id_utils.CouldUpdateRoot(mur) {
				hexRoot := id_utils.HexRoot(potNewRoot)
				if err = AddNewRoot(sc, groupId, hexRoot); err != nil {
					return err
				}
				pushedRoot = true
			} else {
				potNewRoot = mur
				pushedRoot = false
			}
		}
		return sc.CommitTransaction(context.Background())
	})
	return potNewRoot, pushedRoot, err
}

func GetOrPushRootFromUsers(potNewRoot *flatgen.RootT) (*flatgen.RootT, bool, error) {
	var pushedRoot bool
	userId1 := id_utils.HexNodeId(potNewRoot.Primary)
	userId2 := id_utils.HexNodeId(potNewRoot.Secondary)
	fmt.Printf("GetOrPushRootFromUsers: pot_new_root=%s", id_utils.HexRoot(potNewRoot))
	fmt.Printf("GetOrPushRootFromUsers: userId1=%s, userId2=%s\n", userId1, userId2)
	err := db.Mongo.UseSession(context.Background(), func(sc mongo.SessionContext) error {
		err := sc.StartTransaction()
		if err != nil {
			fmt.Println("GetOrPushRootFromUsers: error starting tx:", err)
			return err
		}

		roots, err := readRoots(sc, userId1)
		if err != nil {
			fmt.Println("GetOrPushRootFromUsers: error reading roots:", err)
			return err
		}

		// we want the homological ones
		roots = utils.Filter(roots, func(r *flatgen.RootT) bool {
			return id_utils.IsHomological(r, potNewRoot)
		})

		fmt.Println("GetOrPushRootFromUsers: homological roots:", roots)

		if len(roots) == 0 {
			fmt.Println("GetorPushRootFromUsers: empty roots, adding new")
			hexRoot := id_utils.HexRoot(potNewRoot)
			if err := AddRoots(sc, hexRoot, userId1, userId2); err != nil {
				fmt.Println("GetOrPushRootFromUsers: error adding roots:", err)
				return err
			}
			pushedRoot = true
		} else {
			mur := id_utils.MostUpToDateRoot(roots)
			if id_utils.CouldUpdateRoot(mur) {
				fmt.Println("GetorPushRootFromUsers: could update root")
				hexRoot := id_utils.HexRoot(potNewRoot)
				if err = AddRoots(sc, hexRoot, userId1, userId2); err != nil {
					fmt.Println("GetOrPushRootFromUsers: error adding roots:",
						err)
					return err
				}
				pushedRoot = true
			} else {
				fmt.Println("GetorPushRootFromUsers: not updating root")
				potNewRoot = mur
				pushedRoot = false
			}
		}
		return sc.CommitTransaction(context.Background())
	})
	fmt.Println("GetOrPushRootFromUsers: transaction error? :", err)
	return potNewRoot, pushedRoot, err
}

func GetOrPushRoot(potNewRoot *flatgen.RootT) (*flatgen.RootT, bool, error) {
	potNewRoot.Confirmed = true
	isGroup := id_utils.EqualNodeId(potNewRoot.Secondary, id_utils.NodeIdZero)
	if isGroup {
		log.Println("GetOrPushRoot: isGroup")
		return GetOrPushRootFromGroup(potNewRoot)
	} else {
		log.Println("GetOrPushRoot: isUser")
		return GetOrPushRootFromUsers(potNewRoot)
	}
}
