package handlers

import (
	"context"
	"log"
	"strconv"

	"firebase.google.com/go/v4/messaging"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils/utils"
)

func SendNotifications(ntf *flatgen.Notifications) {
	msgs := makeNotifications(ntf)
	if len(msgs) == 0 {
		return
	}
	br, err := db.Messager.SendEach(context.Background(), msgs)
	utils.NonFatal(err, "Error sending notifications")
	for _, r := range br.Responses {
		utils.NonFatal(r.Error, "Error sending a notification")
	}
}

func makeNotifications(ntf *flatgen.Notifications) []*messaging.Message {
	msgs := make([]*messaging.Message, 0, ntf.TokensLength())
	for i := 0; i < ntf.TokensLength(); i++ {
		log.Printf("token%d=%s\n", i, ntf.Tokens(i))
		m := &messaging.Message{
			// APNS: &messaging.APNSConfig{
			// 	Payload: &messaging.APNSPayload{
			// 		Aps: &messaging.Aps{
			// 			// ContentAvailable: ,
			// 			Alert: &messaging.ApsAlert{},
			// 		},
			// 	},
			// },
			Token: string(ntf.Tokens(i)),
			Data: map[string]string{
				"senderId":      string(ntf.SenderId()),
				"senderName":    string(ntf.SenderName()),
				"root":          string(ntf.Root()),
				"title":         string(ntf.Title()),
				"body":          string(ntf.Body()),
				"senderMediaId": string(ntf.SenderMediaId()),
				"groupMediaId":  string(ntf.GroupMediaId()),
				"isGroup":       strconv.FormatBool(ntf.IsGroup()),
			},
		}
		msgs = append(msgs, m)

	}
	return msgs
}
