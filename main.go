package main

import (
        "log"
        "flag"
        "time"

        "github.com/streadway/amqp"
        "github.com/google/uuid"
)

var url = flag.String("url", "amqp:///", "The AMQP URL to connect to.")
var name = flag.String("name", "", "The name to give this Remit service.")

func init() {
        flag.Parse()
}

func main() {
        printOptions()

        conn := connect(*url)

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")

        body := []byte(`{
                "foo": true,
                "baz": "qux",
                "bool": true,
                "notBool": false
        }`)

        sendMessage(ch, body)
}

func sendMessage(ch *amqp.Channel, body []byte) {
        err := ch.Publish(
                "remit",
                "my.worker.test",
                false,
                false,
                amqp.Publishing{
                        ContentType: "application/json",
                        Body: body,
                        Timestamp: time.Now(),
                        MessageId: uuid.New().String(),
                        AppId: *name,
                },
        )

        failOnError(err, "Failed to publish a message")
        log.Printf(" [x] Sent %s", body)
}

func connect(url string) *amqp.Connection {
        conn, err := amqp.Dial(url)
        failOnError(err, "Failed to connect to RabbitMQ")

        return conn
}

func printOptions() {
        log.Println("AMQP URL is:", *url)
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
        }
}
