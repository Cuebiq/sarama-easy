package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestNewKafkaConfig_Defaults(t *testing.T) {
	conf := NewKafkaConfig()

	if conf.Brokers != "localhost:9092" {
		t.Errorf("expected Brokers 'localhost:9092', got '%s'", conf.Brokers)
	}
	if conf.Version != "1.1.0" {
		t.Errorf("expected Version '1.1.0', got '%s'", conf.Version)
	}
	if conf.Group != "default-group" {
		t.Errorf("expected Group 'default-group', got '%s'", conf.Group)
	}
	if conf.ClientID != "sarama-easy" {
		t.Errorf("expected ClientID 'sarama-easy', got '%s'", conf.ClientID)
	}
	if conf.RebalanceStrategy != "roundrobin" {
		t.Errorf("expected RebalanceStrategy 'roundrobin', got '%s'", conf.RebalanceStrategy)
	}
	if conf.RebalanceTimeout != 1*time.Minute {
		t.Errorf("expected RebalanceTimeout 1m, got %v", conf.RebalanceTimeout)
	}
	if conf.InitOffsets != "latest" {
		t.Errorf("expected InitOffsets 'latest', got '%s'", conf.InitOffsets)
	}
	if conf.CommitInterval != 10*time.Second {
		t.Errorf("expected CommitInterval 10s, got %v", conf.CommitInterval)
	}
	if conf.FlushInterval != 1*time.Second {
		t.Errorf("expected FlushInterval 1s, got %v", conf.FlushInterval)
	}
}

func TestConfigString_MasksPassword(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Username = "admin"
	conf.Password = "secret123"

	str := conf.String()

	if strings.Contains(str, "secret123") {
		t.Error("Config.String() should not contain the actual password")
	}
	if !strings.Contains(str, "****") {
		t.Error("Config.String() should contain masked password '****'")
	}
	if !strings.Contains(str, "admin") {
		t.Error("Config.String() should contain the username")
	}
}

func TestConfigString_EmptyPassword(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Password = ""

	str := conf.String()

	if strings.Contains(str, "****") {
		t.Error("Config.String() should not mask an empty password")
	}
}

func TestConfigureConsumer_ValidConfig(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Topics = "test-topic"

	saramaConf, err := configureConsumer(conf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saramaConf == nil {
		t.Fatal("expected non-nil sarama config")
	}
	if saramaConf.ClientID != "sarama-easy" {
		t.Errorf("expected ClientID 'sarama-easy', got '%s'", saramaConf.ClientID)
	}
	if !saramaConf.Consumer.Return.Errors {
		t.Error("expected Consumer.Return.Errors to be true")
	}
}

func TestConfigureConsumer_InvalidVersion(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Version = "invalid"

	_, err := configureConsumer(conf)
	if err == nil {
		t.Fatal("expected error for invalid Kafka version")
	}
}

func TestConfigureConsumer_InvalidRebalanceStrategy(t *testing.T) {
	conf := NewKafkaConfig()
	conf.RebalanceStrategy = "unknown"

	_, err := configureConsumer(conf)
	if err == nil {
		t.Fatal("expected error for invalid rebalance strategy")
	}
}

func TestConfigureConsumer_InvalidInitOffsets(t *testing.T) {
	conf := NewKafkaConfig()
	conf.InitOffsets = "invalid"

	_, err := configureConsumer(conf)
	if err == nil {
		t.Fatal("expected error for invalid init offsets")
	}
}

func TestConfigureConsumer_RangeStrategy(t *testing.T) {
	conf := NewKafkaConfig()
	conf.RebalanceStrategy = "range"

	saramaConf, err := configureConsumer(conf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saramaConf.Consumer.Group.Rebalance.Strategy == nil {
		t.Error("expected non-nil rebalance strategy")
	}
}

func TestConfigureConsumer_IsolationLevels(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		expected int
	}{
		{"ReadUncommitted", "ReadUncommitted", 0},
		{"ReadCommitted", "ReadCommitted", 1},
		{"default", "", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewKafkaConfig()
			conf.IsolationLevel = tt.level

			saramaConf, err := configureConsumer(conf)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if int(saramaConf.Consumer.IsolationLevel) != tt.expected {
				t.Errorf("expected isolation level %d, got %d", tt.expected, saramaConf.Consumer.IsolationLevel)
			}
		})
	}
}

func TestConfigureConsumer_EarliestOffsets(t *testing.T) {
	conf := NewKafkaConfig()
	conf.InitOffsets = "earliest"

	saramaConf, err := configureConsumer(conf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saramaConf.Consumer.Offsets.Initial != -2 { // sarama.OffsetOldest
		t.Errorf("expected OffsetOldest (-2), got %d", saramaConf.Consumer.Offsets.Initial)
	}
}

func TestConfigureProducer_ValidConfig(t *testing.T) {
	conf := NewKafkaConfig()

	saramaConf, err := configureProducer(conf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saramaConf == nil {
		t.Fatal("expected non-nil sarama config")
	}
	if saramaConf.ClientID != "sarama-easy" {
		t.Errorf("expected ClientID 'sarama-easy', got '%s'", saramaConf.ClientID)
	}
	if !saramaConf.Producer.Return.Errors {
		t.Error("expected Producer.Return.Errors to be true")
	}
	if saramaConf.Producer.Return.Successes {
		t.Error("expected Producer.Return.Successes to be false")
	}
}

func TestConfigureProducer_InvalidVersion(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Version = "not-a-version"

	_, err := configureProducer(conf)
	if err == nil {
		t.Fatal("expected error for invalid Kafka version")
	}
}

func TestConfigureSasl_Disabled(t *testing.T) {
	conf := NewKafkaConfig()
	conf.SaslEnabled = false

	saramaConf, _ := configureConsumer(conf)
	if saramaConf.Net.SASL.Enable {
		t.Error("expected SASL to be disabled")
	}
}

func TestConfigureSasl_EnabledSCRAMSHA256(t *testing.T) {
	conf := NewKafkaConfig()
	conf.SaslEnabled = true
	conf.SaslMechanism = "SCRAM-SHA-256"
	conf.Username = "user"
	conf.Password = "pass"

	saramaConf, _ := configureConsumer(conf)
	if !saramaConf.Net.SASL.Enable {
		t.Error("expected SASL to be enabled")
	}
	if saramaConf.Net.SASL.User != "user" {
		t.Errorf("expected SASL user 'user', got '%s'", saramaConf.Net.SASL.User)
	}
	if saramaConf.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Error("expected SCRAM client generator to be set")
	}
}

func TestConfigureSasl_EnabledSCRAMSHA512(t *testing.T) {
	conf := NewKafkaConfig()
	conf.SaslEnabled = true
	conf.SaslMechanism = "SCRAM-SHA-512"
	conf.Username = "user"
	conf.Password = "pass"

	saramaConf, _ := configureConsumer(conf)
	if !saramaConf.Net.SASL.Enable {
		t.Error("expected SASL to be enabled")
	}
	if saramaConf.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Error("expected SCRAM client generator to be set for SHA512")
	}
}

func TestConfigureTLS_DisabledDoesNothing(t *testing.T) {
	conf := Config{TLSEnabled: false}
	saramaConf := sarama.NewConfig()

	err := configureTLS(conf, saramaConf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saramaConf.Net.TLS.Config != nil {
		t.Error("expected nil TLS config when TLS is disabled")
	}
}

func TestConfigureTLS_EnabledMissingCerts(t *testing.T) {
	tests := []struct {
		name string
		conf Config
	}{
		{"missing all", Config{TLSEnabled: true}},
		{"missing cert and key", Config{TLSEnabled: true, CACerts: "/tmp/ca.pem"}},
		{"missing ca and key", Config{TLSEnabled: true, TLSCert: "/tmp/cert.pem"}},
		{"missing ca and cert", Config{TLSEnabled: true, TLSKey: "/tmp/key.pem"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saramaConf := sarama.NewConfig()
			err := configureTLS(tt.conf, saramaConf)
			if err == nil {
				t.Fatal("expected error when TLS enabled with missing cert fields")
			}
			if !strings.Contains(err.Error(), "TLS enabled but") {
				t.Errorf("unexpected error message: %s", err)
			}
		})
	}
}
