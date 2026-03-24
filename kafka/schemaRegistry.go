package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/linkedin/goavro/v2"
)

// SchemaRegistryClientInterface defines the contract for clients that interact
// with Confluent Schema Registry. Both SchemaRegistryClient and
// CachedSchemaRegistryClient implement this interface.
type SchemaRegistryClientInterface interface {
	GetSchema(int) (*goavro.Codec, error)
	GetSubjects() ([]string, error)
	GetVersions(string) ([]int, error)
	GetSchemaByVersion(string, int) (*goavro.Codec, error)
	GetLatestSchema(string) (*goavro.Codec, error)
	CreateSubject(string, *goavro.Codec) (int, error)
	IsSchemaRegistered(string, *goavro.Codec) (int, error)
	DeleteSubject(string) error
	DeleteVersion(string, int) error
}

// SchemaRegistryClient is an HTTP client for the Confluent Schema Registry REST API.
// It supports automatic retries on 5XX errors with round-robin server selection.
type SchemaRegistryClient struct {
	SchemaRegistryConnect []string
	httpClient            *http.Client
	retries               int
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

type schemaVersionResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	ID      int    `json:"id"`
}

type idResponse struct {
	ID int `json:"id"`
}

const (
	schemaByID       = "/schemas/ids/%d"
	subjects         = "/subjects"
	subjectVersions  = "/subjects/%s/versions"
	deleteSubject    = "/subjects/%s"
	subjectByVersion = "/subjects/%s/versions/%s"

	latestVersion = "latest"

	contentType = "application/vnd.schemaregistry.v1+json"

	defaultTimeout = 2 * time.Second
)

// NewSchemaRegistryClient creates a client to talk with the schema registry at the connect string.
// By default it will retry failed requests (5XX responses and http errors) len(connect) number of times.
func NewSchemaRegistryClient(connect []string) *SchemaRegistryClient {
	return NewSchemaRegistryClientWithOptions(connect, len(connect), 0)
}

// NewSchemaRegistryClientWithRetries creates an http client with a configurable amount of retries on 5XX responses.
func NewSchemaRegistryClientWithRetries(connect []string, retries int) *SchemaRegistryClient {
	return NewSchemaRegistryClientWithOptions(connect, retries, 0)
}

// NewSchemaRegistryClientWithOptions creates a client with configurable retries and timeout.
// A zero timeout uses the default of 2 seconds.
func NewSchemaRegistryClientWithOptions(connect []string, retries int, timeout time.Duration) *SchemaRegistryClient {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	client := &http.Client{
		Timeout: timeout,
	}
	return &SchemaRegistryClient{connect, client, retries}
}

// GetSchema returns a goavro.Codec by unique id
func (client *SchemaRegistryClient) GetSchema(id int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(schemaByID, id), nil)
	if err != nil {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// GetSubjects returns a list of all subjects in the schema registry
func (client *SchemaRegistryClient) GetSubjects() ([]string, error) {
	resp, err := client.httpCall("GET", subjects, nil)
	if err != nil {
		return []string{}, err
	}
	var result = []string{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

// GetVersions returns a list of the versions of a subject
func (client *SchemaRegistryClient) GetVersions(subject string) ([]int, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectVersions, subject), nil)
	if err != nil {
		return []int{}, err
	}
	var result = []int{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

func (client *SchemaRegistryClient) getSchemaByVersionInternal(subject string, version string) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectByVersion, subject, version), nil)
	if err != nil {
		return nil, err
	}
	var schema = new(schemaVersionResponse)
	err = json.Unmarshal(resp, &schema)
	if err != nil {
		return nil, err
	}

	return goavro.NewCodec(schema.Schema)
}

// GetSchemaByVersion returns a goavro.Codec for the version of the subject
func (client *SchemaRegistryClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.getSchemaByVersionInternal(subject, fmt.Sprintf("%d", version))
}

// GetLatestSchema returns a goavro.Codec for the latest version of the subject
func (client *SchemaRegistryClient) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.getSchemaByVersionInternal(subject, latestVersion)
}

// CreateSubject adds a schema to the subject
func (client *SchemaRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// IsSchemaRegistered tests if the schema is registered, if so it returns the unique id of that schema
func (client *SchemaRegistryClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(deleteSubject, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// DeleteSubject deletes a subject. It should only be used in development
func (client *SchemaRegistryClient) DeleteSubject(subject string) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(deleteSubject, subject), nil)
	return err
}

// DeleteVersion deletes a subject. It should only be used in development
func (client *SchemaRegistryClient) DeleteVersion(subject string, version int) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(subjectByVersion, subject, fmt.Sprintf("%d", version)), nil)
	return err
}

func parseSchema(str []byte) (*schemaResponse, error) {
	var schema = new(schemaResponse)
	err := json.Unmarshal(str, &schema)
	return schema, err
}

func parseID(str []byte) (int, error) {
	var id = new(idResponse)
	err := json.Unmarshal(str, &id)
	return id.ID, err
}

func (client *SchemaRegistryClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	nServers := len(client.SchemaRegistryConnect)
	offset := rand.Intn(nServers)
	for i := 0; ; i++ {
		url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect[(i+offset)%nServers], uri)
		req, err := http.NewRequest(method, url, payload)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		resp, err := client.httpClient.Do(req)
		if i < client.retries && (err != nil || retriable(resp)) {
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		body, err := func() ([]byte, error) {
			defer resp.Body.Close()
			if !okStatus(resp) {
				return nil, newError(resp)
			}
			return io.ReadAll(resp.Body)
		}()
		return body, err
	}
}

func retriable(resp *http.Response) bool {
	return resp.StatusCode >= 500 && resp.StatusCode < 600
}

func okStatus(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
