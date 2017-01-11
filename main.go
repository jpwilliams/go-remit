package main

import (
        "log"
        "flag"
        "encoding/json"

        "github.com/streadway/amqp"
)

var url = flag.String("jack", "amqp:///", "The AMQP URL to connect to.")

func init() {
        flag.Parse()
}

func main() {
        printOptions()

        conn := connect(*url)

        ch, err := conn.Channel()
        failOnError(err, "Failed to open a channel")

        body, _ := json.Marshal(map[string]string{
                "foo": "bar",
                "baz": "qux",
        })

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
                        Body: []byte(body),
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
