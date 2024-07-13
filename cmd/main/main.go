package main

import (
	"log"
	"net"
	"net/http"

	"github.com/coldstar-507/chat-server/internal/db"
	"github.com/coldstar-507/chat-server/internal/handlers"
	"github.com/coldstar-507/utils"
)

func startChatServer() {
	listener, err := net.Listen("tcp", ":11002")
	utils.Panic(err, "startChatServer error on net.Listen")
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
		} else {
			log.Println("new connection:", conn.LocalAddr())
		}
		go handlers.HandleChatConn(conn)
	}
}

func main() {
	log.Println("Initializing LevelDb")
	db.InitLevelDb()
	defer db.ShutDownLevelDb()

	log.Println("Starting tcp chat-server on port 11002")
	go startChatServer()

	log.Println("Starting device connections manager")
	go handlers.DevConnsManager.Run()
	log.Println("Starting chat connections manager")
	go handlers.ChatConnsManager.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", handlers.HandlePing)
	mux.HandleFunc("GET /dev-conn/{ref}", handlers.HandleDevConn)
	mux.HandleFunc("POST /push", handlers.HandlePush)

	server := utils.ApplyMiddlewares(mux,
		utils.HttpLogging,
		utils.StatusLogger)

	log.Println("Starting http chat-server on port 8082")
	err := http.ListenAndServe("0.0.0.0:8082", server)

	utils.NonFatal(err, "http.ListenAndServe error")
}
