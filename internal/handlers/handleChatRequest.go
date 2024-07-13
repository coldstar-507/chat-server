package handlers

import (
	"context"
	"log"

	"firebase.google.com/go/v4/messaging"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/flatgen"
	"github.com/coldstar-507/utils"
)

func SendNotifications(ntf *flatgen.Notifications) {
	msgs := makeNotifications(ntf)
	br, err := db.Messager.SendEach(context.Background(), msgs)
	utils.NonFatal(err, "Error sending notifications")
	for _, r := range br.Responses {
		utils.NonFatal(r.Error, "Error sending a notification")
	}
}

func makeNotifications(ntf *flatgen.Notifications) []*messaging.Message {
	msgs := make([]*messaging.Message, 0, ntf.TargetsLength())
	var t *flatgen.MessageTarget
	for i := 0; i < ntf.TargetsLength(); i++ {
		if !ntf.Targets(t, i) {
			log.Printf("WARNING makenotifications target[%d]\n", i)
			continue
		}
		if t.ShowNotif() {
			m := &messaging.Message{
				Token: string(t.Token()),
				Data: map[string]string{
					"b": string(ntf.Body()),
					"h": string(ntf.Header()),
					"r": string(ntf.Root()),
					"s": string(ntf.Sender()),
				},
			}
			msgs = append(msgs, m)
		}
	}
	return msgs
}
