package handlers

type CHAT_HEADERS = byte

const (
	CHAT_EVENT CHAT_HEADERS = iota
	MESSAGE_SENT
	SCROLL_REQUEST
	MESSAGE_SAVE_ERROR
	CHAT_SCROLL_DONE
	NOTIFICATIONS
	CHAT_CONN_REQ
	CHAT_DISC_REQ
)
