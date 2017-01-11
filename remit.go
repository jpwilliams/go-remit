package remit

import (
        "log"
        "flag"
        "encoding/json"
        // "time"

        "github.com/streadway/amqp"
        "github.com/google/uuid"
)

var url = flag.String("url", "amqp:///", "The AMQP URL to connect to.")
var name = flag.String("name", "", "The name to give this Remit service.")

func init() {
        flag.Parse()

        log.Println("URL is:", *url)
        log.Println("Name is:", *name)
}

type Config struct {
        Name *string
        Url *string
}

type Session struct {
        config Config
        connection *amqp.Connection
        workChannel *amqp.Channel
        publishChannel *amqp.Channel
        consumeChannel *amqp.Channel
}

type Endpoint struct {
        RoutingKey string
        Queue string
}

type Event struct {
        EventId string
        EventType string
        Resource string
        Data EventData
        Callback EventCallback
}

type EventData map[string]interface{}
type EventCallback func()

func Connect() (Session) {
        conn, err := amqp.Dial(*url)
        failOnError(err, "Failed to connect to RabbitMQ")

        workChannel, err := conn.Channel()
        failOnError(err, "Failed to open work channel")

        publishChannel, err := conn.Channel()
        failOnError(err, "Failed to open publish channel")

        consumeChannel, err := conn.Channel()
        failOnError(err, "Failed to open consume channel")

        return Session{
                config: Config{
                        Name: name,
                        Url: url,
                },

                connection: conn,
                workChannel: workChannel,
                publishChannel: publishChannel,
                consumeChannel: consumeChannel,
        }
}

func(session *Session) Endpoint(key string) (Endpoint) {
        log.Println("Adding endpoint for", key, session)

        return Endpoint{
                RoutingKey: key,
                Queue: key,
        }
}

func(endpoint *Endpoint) Data(handler func(Event)) {
        log.Println("Data listener added")

        parsedData := EventData{}
        err := json.Unmarshal([]byte(`{"foo": "bar"}`), &parsedData)
        failOnError(err, "Failed to parse JSON")

        handler(Event{
                EventId: uuid.New().String(),
                EventType: "a.message",
                Resource: "that.publisher",
                Data: parsedData,
        })
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
        }
}

// func init() {
//         flag.Parse()
// }

// func main() {
//         printOptions()

//         conn := connect(*url)

//         ch, err := conn.Channel()
//         failOnError(err, "Failed to open a channel")

//         body := []byte(`{
//                 "foo": true,
//                 "baz": "qux",
//                 "bool": true,
//                 "notBool": false
//         }`)

//         sendMessage(ch, body)
// }

// func SendMessage(ch *amqp.Channel, body []byte) {
//         err := ch.Publish(
//                 "remit",
//                 "my.worker.test",
//                 false,
//                 false,
//                 amqp.Publishing{
//                         ContentType: "application/json",
//                         Body: body,
//                         Timestamp: time.Now(),
//                         MessageId: uuid.New().String(),
//                         AppId: *name,
//                 },
//         )

//         failOnError(err, "Failed to publish a message")
//         log.Printf(" [x] Sent %s", body)
// }

// func Connect() *amqp.Connection, *amqp.Channel {
//         conn, err := amqp.Dial(*url)
//         failOnError(err, "Failed to connect to RabbitMQ")

//         ch, err := conn.Channel()
//         failOnError(err, "Failed to open a channel")

//         return conn
// }

// func printOptions() {
//         log.Println("AMQP URL is:", *url)
// }

