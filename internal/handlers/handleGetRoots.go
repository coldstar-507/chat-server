package handlers

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils2"
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

	roots := utils2.Map(res.Roots, func(r string) *flatgen.RootT {
		return utils2.ParseRoot(hex.NewDecoder(strings.NewReader(r)))
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
	groupId := utils2.HexNodeId(potNewRoot.Primary)
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
			err = AddNewRoot(sc, groupId, utils2.HexRoot(potNewRoot))
			if err != nil {
				return err
			}
			pushedRoot = true
		} else {
			mur := utils2.MostUpToDateRoot(roots)
			if utils2.CouldUpdateRoot(mur) {
				hexRoot := utils2.HexRoot(potNewRoot)
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
	userId1 := utils2.HexNodeId(potNewRoot.Primary)
	userId2 := utils2.HexNodeId(potNewRoot.Secondary)
	fmt.Printf("GetOrPushRootFromUsers: pot_new_root=%s", utils2.HexRoot(potNewRoot))
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
		roots = utils2.Filter(roots, func(r *flatgen.RootT) bool {
			return utils2.IsHomological(r, potNewRoot)
		})

		fmt.Println("GetOrPushRootFromUsers: homological roots:", roots)

		if len(roots) == 0 {
			fmt.Println("GetorPushRootFromUsers: empty roots, adding new")
			hexRoot := utils2.HexRoot(potNewRoot)
			if err := AddRoots(sc, hexRoot, userId1, userId2); err != nil {
				fmt.Println("GetOrPushRootFromUsers: error adding roots:", err)
				return err
			}
			pushedRoot = true
		} else {
			mur := utils2.MostUpToDateRoot(roots)
			if utils2.CouldUpdateRoot(mur) {
				fmt.Println("GetorPushRootFromUsers: could update root")
				hexRoot := utils2.HexRoot(potNewRoot)
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
	isGroup := utils2.EqualNodeId(potNewRoot.Secondary, utils2.NodeIdZero)
	if isGroup {
		log.Println("GetOrPushRoot: isGroup")
		return GetOrPushRootFromGroup(potNewRoot)
	} else {
		log.Println("GetOrPushRoot: isUser")
		return GetOrPushRootFromUsers(potNewRoot)
	}
}
