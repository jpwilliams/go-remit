package remit

type Event struct {
        EventId         string
        EventType       string
        Resource        string
        Data            EventData
        Callback        EventCallback
}

type EventData map[string]interface{}
type EventCallback func()
