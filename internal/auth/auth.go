package myAuth

import (
	"context"
	"fmt"
	"log"

	"firebase.google.com/go/v4/auth"

	db "github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/utils/utils"
)

var (
	authClient *auth.Client
)

type TokenManager struct {
	tokenCh chan *VerificationRequest
}

type VerificationRequest struct {
	verificationId string
	responseCode   chan *auth.Token
}

func InitFirebaseAuth() {
	auth, err := db.FirebaseApp.Auth(context.Background())
	utils.Fatal(err, "TokenManager.Run(): error initialzing auth client")
	authClient = auth
}

func (tm *TokenManager) Run() {
	for {
		token := <-tm.tokenCh
		go verifyToken(token)
	}
}

// func DeleteAllUsers() {
// 	pageToken := ""
// 	for {
// 		users := authClient.Users(context.Background(), pageToken)
// 		for {
// 			user, err := users.Next()
// 			if err != nil {
// 				log.Println("users.Next() returned error:", err)
// 				break
// 			}
// 			if user == nil {
// 				log.Println("users.Next() returned nil user:")
// 				break
// 			}
// 			err = authClient.DeleteUser(context.Background(), user.UID)
// 			if err != nil {
// 				log.Printf("error deleting user %s: %v", user.UID, err)
// 			} else {
// 				log.Printf("Deleted user: %s", user.UID)
// 			}

// 		}
// 		pageToken = users.PageInfo().Token
// 		// If there are more users, fetch the next batch
// 		if pageToken == "" {
// 			log.Printf("done deleting users")
// 			break
// 		}
// 	}
// }

func verifyToken(vr *VerificationRequest) {
	token, err := authClient.VerifyIDToken(context.Background(), vr.verificationId)
	if err != nil {
		log.Fatalf("Error verifying token: %v", err)
		vr.responseCode <- nil
	} else {
		vr.responseCode <- token
	}
	fmt.Println("User UID:", token.UID)
}
