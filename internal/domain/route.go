package domain

// ConversationRoute is private local metadata, never a replicated event.
type ConversationRoute struct {
	ThreadID         string `json:"thread_id"`
	TaskID           string `json:"task_id,omitempty"`
	OwnerAgentID     string `json:"owner_agent_id"`
	Platform         string `json:"platform"`
	ChatID           string `json:"chat_id"`
	PlatformThreadID string `json:"platform_thread_id,omitempty"`
	HermesSessionID  string `json:"hermes_session_id,omitempty"`
	HermesSessionKey string `json:"hermes_session_key,omitempty"`
	ProfileName      string `json:"profile_name,omitempty"`
	TransportProfile string `json:"transport_profile,omitempty"`
	ReplyPolicy      string `json:"reply_policy"`
}
