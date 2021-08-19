package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"github.com/spf13/viper"
	"io/ioutil"
	"strings"
	"time"
)

type Config struct {
	BootstrapServers string
	ApiKey           string
	ApiSecret        string
	Hostname         string

	SchemaRegistryURL       string
	SchemaRegistryAPIKey    string
	SchemaRegistryAPISecret string
}

type Kafka struct {
	config       Config
	SchemaClient *srclient.SchemaRegistryClient
}

type ConsumerConfig struct {
	Group          string
	Topics         []string
	OverrideConfig map[string]interface{}
	ReadTimeout    time.Duration
}

type ProducerConfig struct {
	Topic          string
	OverrideConfig map[string]interface{}
	SchemaPath     string
}

func DefaultConfig() Config {
	config := Config{}
	config.BootstrapServers = viper.GetString("KAFKA_BROKERS")
	config.ApiKey = viper.GetString("KAFKA_API_KEY")
	config.ApiSecret = viper.GetString("KAFKA_API_SECRET")

	hostname := viper.GetString("HOSTNAME")
	if hostname == "" {
		hostname = "dev-" + uuid.New().String()
	}
	config.Hostname = hostname

	config.SchemaRegistryURL = viper.GetString("KAFKA_SCHEMA_URL")
	config.SchemaRegistryAPIKey = viper.GetString("KAFKA_SCHEMA_API_KEY")
	config.SchemaRegistryAPISecret = viper.GetString("KAFKA_SCHEMA_API_SECRET")
	return config
}

func DefaultConsumerConfig() ConsumerConfig {
	group := viper.GetString("KAFKA_CONSUMER_GROUP")
	if group == "" {
		group = "dev-local-group"
	}
	topics := strings.Split(viper.GetString("KAFKA_CONSUMER_TOPICS"), ",")
	config := viper.GetStringMap("KAFKA_CONSUMER_CONFIG")
	readTimeoutSeconds := viper.GetInt("KAFKA_CONSUMER_READ_TIMEOUT_SECONDS")
	if readTimeoutSeconds == 0 {
		readTimeoutSeconds = 20
	}
	readTimeout := time.Second * time.Duration(readTimeoutSeconds)
	return ConsumerConfig{
		Group:          group,
		Topics:         topics,
		OverrideConfig: config,
		ReadTimeout:    readTimeout,
	}
}

func DefaultProducerConfig() ProducerConfig {
	topic := viper.GetString("KAFKA_PRODUCER_TOPIC")
	config := viper.GetStringMap("KAFKA_PRODUCER_CONFIG")
	schemaPath := viper.GetString("KAFKA_SCHEMA_PATH")

	return ProducerConfig{
		Topic:          topic,
		OverrideConfig: config,
		SchemaPath:     schemaPath,
	}
}

func NewKafka(config Config) (*Kafka, error) {
	k := Kafka{}
	k.config = config

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(config.SchemaRegistryURL)
	if config.SchemaRegistryAPIKey != "" || config.SchemaRegistryAPISecret != "" {
		schemaRegistryClient.SetCredentials(config.SchemaRegistryAPIKey, config.SchemaRegistryAPISecret)
	}
	k.SchemaClient = schemaRegistryClient

	return &k, nil
}

func (k *Kafka) NewConsumer(config ConsumerConfig) (*Consumer, error) {
	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":  k.config.BootstrapServers,
		"group.id":           config.Group,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"client.id":          k.config.Hostname,
	}

	k.addAuth(consumerConf)
	k.addConfig(consumerConf, config.OverrideConfig)

	consumer, err := kafka.NewConsumer(consumerConf)
	if err != nil {
		return nil, err
	}

	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer, config.ReadTimeout, k.SchemaClient}, nil
}

func (k *Kafka) NewProducer(config ProducerConfig) (*Producer, error) {
	producerConf := &kafka.ConfigMap{
		"bootstrap.servers":   k.config.BootstrapServers,
		"go.delivery.reports": false,
	}

	k.addAuth(producerConf)
	k.addConfig(producerConf, config.OverrideConfig)

	producer, err := kafka.NewProducer(producerConf)
	if err != nil {
		return nil, err
	}

	schema, err := k.SchemaClient.GetLatestSchema(config.Topic)
	if err != nil {
		schemaBytes, _ := ioutil.ReadFile(config.SchemaPath)
		schema, err = k.SchemaClient.CreateSchema(config.Topic, string(schemaBytes), srclient.Avro)
		if err != nil {
			return nil, err
		}
	}

	return &Producer{config.Topic, producer, schema}, nil
}

func (k *Kafka) addAuth(config *kafka.ConfigMap) {
	apiKey := k.config.ApiKey
	apiSecret := k.config.ApiSecret
	if apiKey != "" && apiSecret != "" {
		auth := map[string]interface{}{
			"security.protocol":                   "SASL_SSL",
			"sasl.mechanisms":                     "PLAIN",
			"sasl.username":                       apiKey,
			"sasl.password":                       apiSecret,
			"enable.ssl.certificate.verification": false,
		}
		k.addConfig(config, auth)
	}
}

func (k *Kafka) addConfig(config *kafka.ConfigMap, data map[string]interface{}) {
	configMap := *config
	for key, value := range data {
		configMap[key] = value
	}
	config = &configMap
}
