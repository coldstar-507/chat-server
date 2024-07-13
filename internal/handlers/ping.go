package handlers

import (
	"net/http"
)

func HandlePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong\n"))
}
