Consume avro / non-avro Kafka topics easily

# Build

`go build`

# Run

`./kafka-console-consumer -topic my-json-topic`

`./kafka-console-consumer -topic my-avro-topic`


# Arguments

```
Available command line options:
  -brokers string
        The comma separated list of brokers in the Kafka cluster (default "kafka.service.consul:9092")
  -buffer-size int
        The buffer size of the message channel. (default 256)
  -offset oldest
        The offset to start with. Can be oldest, `newest` (default "newest")
  -partitions string
        The partitions to consume, can be 'all' or comma-separated numbers (default "all")
  -schema-registry string
        The URI of the schema registry (default "http://schema-registry.service.consul")
  -template string
        go template (default "{{ .Offset}} [{{.Partition}}]:{{ .Key}}, {{ .Value }}")
  -topic string
        REQUIRED: the topic to consume
  -verbose
        Whether to turn on sarama logging

```
