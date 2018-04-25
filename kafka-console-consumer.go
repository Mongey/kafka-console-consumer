package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	template "text/template"

	schemaRegistry "github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

var (
	brokerList        = flag.String("brokers", "kafka.service.consul:9092", "The comma separated list of brokers in the Kafka cluster")
	topic             = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions        = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset            = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose           = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize        = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	schemaRegistryURI = flag.String("schema-registry", "http://schema-registry.service.consul", "The URI of the schema registry")
	temp              = flag.String("template", "{{ .Offset}} [{{.Partition}}]:{{ .Key}}, {{ .Value }}", "go template")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

type Message struct {
	Key       string
	Value     string
	Offset    int64
	Partition int32
	SchemaID  int
}

type schemaAndMessage struct {
	Message  string
	SchemaID int
}

var schemaCache = map[int]string{}

func main() {
	flag.Parse()

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(strings.Split(*brokerList, ","), nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	newlineTemplate := *temp + "\n"
	t, err := template.New("t1").Parse(newlineTemplate)
	if err != nil {
		panic("Template is invalid")

	}
	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	var m Message
	sr, _ := schemaRegistry.NewClient(*schemaRegistryURI)
	go func() {
		for msg := range messages {
			k, err := decodeAvro(msg.Key, sr)
			if err != nil {
				fmt.Println("key de err:", err)
				continue
			}
			v, err := decodeAvro(msg.Value, sr)
			if err != nil {
				fmt.Println("v de err:", err)
				continue
			}
			m = Message{
				SchemaID:  v.SchemaID,
				Key:       k.Message,
				Value:     v.Message,
				Partition: msg.Partition,
				Offset:    msg.Offset,
			}
			t.Execute(os.Stdout, m)
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func decodeAvro(in []byte, sr schemaRegistry.Client) (*schemaAndMessage, error) {
	if len(in) > 0 && in[0] == 0 {
		schemaIDb := in[1:5]
		data := in[5:]
		i := int(binary.BigEndian.Uint32(schemaIDb))
		schema, err := getSchema(sr, i)
		if err != nil {
			return nil, nil
		}
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, nil
		}
		native, _, err := codec.NativeFromBinary(data)
		if err != nil {
			fmt.Println(err)
		}
		b, err := json.Marshal(native)
		if err != nil {
			return nil, err
		}
		return &schemaAndMessage{
			Message:  string(b),
			SchemaID: i,
		}, nil
	}
	return &schemaAndMessage{
		Message:  string(in),
		SchemaID: -1,
	}, nil
}

func getSchema(sr schemaRegistry.Client, id int) (string, error) {
	if val, ok := schemaCache[id]; ok {
		return val, nil
	}
	schema, err := sr.GetSchemaById(id)
	if err != nil {
		return "", nil
	}
	schemaCache[id] = schema
	return schema, nil
}
