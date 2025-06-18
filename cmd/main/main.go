package main

import (
	"log"
	"net/http"

	// myAuth "github.com/coldstar-507/chat-server/internal/auth"
	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/chat-server/internal/handlers"
	"github.com/coldstar-507/router-server/router_utils"
	"github.com/coldstar-507/utils2"
)

// this should be part of the ENV in production
var (
	ip         string                   = "localhost"
	place      router_utils.SERVER_NUMB = 4000
	routerType router_utils.ROUTER_TYPE = router_utils.CHAT_ROUTER
)

func main() {
	// log.Println("Initializing LevelDb")
	// db.InitLevelDb()
	log.Println("Initializing Firebase App")
	db.InitFirebaseApp()

	log.Println("Initializing Firebase Messager")
	db.InitFirebaseMessager()
	// defer db.ShutDownLevelDb()

	// log.Println("Initializing Firebase Auth")
	// myAuth.InitFirebaseAuth()
	// log.Println("Deleting all firebase auth users")
	// myAuth.DeleteAllUsers()

	log.Println("Initializing ScyllaDb")
	db.InitScylla()
	defer db.ShutdownScylla()

	log.Println("Initializing Mongodb")
	db.InitMongo()
	defer db.ShutdownMongo()

	log.Println("Starting tcp chat-server on port 11002")
	// go handlers.StartChatServer()
	go handlers.StartChatServer2()

	log.Println("starting tcp boost-server on port 11003")
	go handlers.StartBoostServer()

	log.Println("starting tcp device-server on port 11004")

	go handlers.StartDeviceServer()
	log.Println("Starting device connections manager")
	go handlers.DevConnsManager.Run()

	// log.Println("Starting chat connections manager")
	// go handlers.ChatConnsManager.Run()

	// log.Println("Starting chat client manager")
	// go handlers.ConnManager.Run()

	// log.Println("Starting root manager")
	// go handlers.RM.Run()

	log.Println("Starting local router")
	router_utils.InitLocalServer(ip, place, routerType)
	go router_utils.LocalServer.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", router_utils.HandlePing)
	mux.HandleFunc("GET /route-scores", router_utils.HandleScoreRequest)
	mux.HandleFunc("GET /local-router", router_utils.HandleServerStatus)
	mux.HandleFunc("GET /full-router", router_utils.HandleRouterStatus)

	// mux.HandleFunc("POST /dev-conn", handlers.HandleDevConn)
	mux.HandleFunc("POST /get-message", handlers.HandleGetMsg)
	mux.HandleFunc("POST /push", handlers.HandlePush)

	server := utils2.ApplyMiddlewares(mux, utils2.StatusLogger)

	addr := "0.0.0.0:8082"
	log.Println("Starting http chat-server on", addr)
	err := http.ListenAndServe(addr, server)
	utils2.NonFatal(err, "http.ListenAndServe error")
}
