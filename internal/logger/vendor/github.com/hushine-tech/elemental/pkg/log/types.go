package log

type LogType string

const (
	TypeSystem   LogType = "system"
	TypeAccess   LogType = "access"
	TypeExtAPI   LogType = "ext_api"
	TypeWebSocket LogType = "websocket"
	TypeSQL      LogType = "sql"
	TypeRoot     LogType = "root"
)

func (t LogType) String() string {
	return string(t)
}

func (t LogType) FileName() string {
	return string(t) + ".log"
}

func ParseLogType(s string) (LogType, bool) {
	switch s {
	case "system":
		return TypeSystem, true
	case "access":
		return TypeAccess, true
	case "ext_api":
		return TypeExtAPI, true
	case "websocket":
		return TypeWebSocket, true
	case "sql":
		return TypeSQL, true
	case "root":
		return TypeRoot, true
	default:
		return TypeRoot, false
	}
}

type AccessLogEntry struct {
	Method        string
	Path          string
	RequestHeader map[string]string
	RequestParams map[string]any
	RequestBody   string
	ResponseBody  string
	HTTPStatus    int
	LatencyMs     int64
}

type ExtAPILogEntry struct {
	APIName       string
	URL           string
	FullURL       string
	RequestHeader map[string]string
	RequestParams map[string]any
	RequestBody   string
	ResponseBody  string
	HTTPStatus    int
	LatencyMs     int64
}

type SQLLogEntry struct {
	Statement    string
	RowsAffected int64
	LatencyMs    int64
}

type WebSocketLogEntry struct {
	URL       string
	FullURL   string
	EventType string
	Direction string
	Frame     string
	LatencyMs int64
}
