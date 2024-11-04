package main

import (
	"log"
	"net/http"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/chat-server/internal/handlers"
	"github.com/coldstar-507/utils"
)

// this should be part of the ENV in production
var (
	ip    string            = "localhost"
	place uint16            = 0x1000
	st    utils.SERVER_TYPE = utils.CHAT_ROUTER
)

func main() {
	log.Println("Initializing LevelDb")
	db.InitLevelDb()
	defer db.ShutDownLevelDb()

	log.Println("Starting tcp chat-server on port 11002")
	go handlers.StartChatServer()

	log.Println("starting tcp boost-server on port 11003")
	go handlers.StartBoostServer()

	log.Println("starting tcp device-server on port 11004")
	go handlers.StartDeviceServer()
	log.Println("Starting device connections manager")
	go handlers.DevConnsManager.Run()

	// log.Println("Starting chat connections manager")
	// go handlers.ChatConnsManager.Run()

	log.Println("Starting chat client manager")
	go handlers.ConnManager.Run()

	log.Println("Starting local router")
	utils.InitLocalRouter(ip, st, place)
	go utils.LocalRouter.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", utils.HandlePing)
	mux.HandleFunc("GET /route-scores", utils.HandleScoreRequest)

	// mux.HandleFunc("POST /dev-conn", handlers.HandleDevConn)
	mux.HandleFunc("POST /push", handlers.HandlePush)

	server := utils.ApplyMiddlewares(mux,
		// utils.HttpLogging,
		utils.StatusLogger,
	)

	addr := "0.0.0.0:8082"
	log.Println("Starting http chat-server on", addr)
	err := http.ListenAndServe(addr, server)
	utils.NonFatal(err, "http.ListenAndServe error")
}
