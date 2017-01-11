package remit

import (
        "flag"

        "github.com/streadway/amqp"
        "github.com/chuckpreslar/emission"
)

var url = flag.String("url", "amqp:///", "The AMQP URL to connect to.")
var name = flag.String("name", "", "The name to give this Remit service.")

func init() {
        flag.Parse()
}

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
                emitter: emission.NewEmitter(),

                EndpointGlobal: EndpointGlobal{
                        emitter: emission.NewEmitter(),
                },
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

