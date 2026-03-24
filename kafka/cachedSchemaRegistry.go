package kafka

import (
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

type schemaCacheEntry struct {
	codec     *goavro.Codec
	fetchedAt time.Time
}

type idCacheEntry struct {
	id        int
	fetchedAt time.Time
}

// CachedSchemaRegistryClient wraps SchemaRegistryClient with thread-safe caching
// for schema lookups (by ID) and schema registrations (by JSON). This avoids
// redundant HTTP calls when the same schema is requested multiple times.
// When cacheTTL is set, entries expire after the given duration and are re-fetched.
type CachedSchemaRegistryClient struct {
	schemaRegistryClient *SchemaRegistryClient
	schemaCache          map[int]*schemaCacheEntry
	schemaCacheLock      sync.RWMutex
	schemaIdCache        map[string]*idCacheEntry
	schemaIdCacheLock    sync.RWMutex
	cacheTTL             time.Duration
}

func NewCachedSchemaRegistryClient(connect []string) *CachedSchemaRegistryClient {
	return NewCachedSchemaRegistryClientWithOptions(connect, len(connect), 0, 0)
}

func NewCachedSchemaRegistryClientWithRetries(connect []string, retries int) *CachedSchemaRegistryClient {
	return NewCachedSchemaRegistryClientWithOptions(connect, retries, 0, 0)
}

// NewCachedSchemaRegistryClientWithOptions creates a cached client with configurable retries,
// HTTP timeout, and cache TTL. Zero values use defaults (2s timeout, no TTL expiration).
func NewCachedSchemaRegistryClientWithOptions(connect []string, retries int, timeout time.Duration, cacheTTL time.Duration) *CachedSchemaRegistryClient {
	srClient := NewSchemaRegistryClientWithOptions(connect, retries, timeout)
	return &CachedSchemaRegistryClient{
		schemaRegistryClient: srClient,
		schemaCache:          make(map[int]*schemaCacheEntry),
		schemaIdCache:        make(map[string]*idCacheEntry),
		cacheTTL:             cacheTTL,
	}
}

func (client *CachedSchemaRegistryClient) isExpired(fetchedAt time.Time) bool {
	return client.cacheTTL > 0 && time.Since(fetchedAt) > client.cacheTTL
}

// GetSchema will return and cache the codec with the given id.
// Uses double-check locking to prevent duplicate fetches under contention.
func (client *CachedSchemaRegistryClient) GetSchema(id int) (*goavro.Codec, error) {
	// Fast path: read lock only
	client.schemaCacheLock.RLock()
	entry := client.schemaCache[id]
	client.schemaCacheLock.RUnlock()
	if entry != nil && !client.isExpired(entry.fetchedAt) {
		return entry.codec, nil
	}

	// Slow path: acquire write lock and double-check before fetching
	client.schemaCacheLock.Lock()
	defer client.schemaCacheLock.Unlock()

	// Another goroutine may have populated the cache while we waited for the lock
	if entry = client.schemaCache[id]; entry != nil && !client.isExpired(entry.fetchedAt) {
		return entry.codec, nil
	}

	codec, err := client.schemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	client.schemaCache[id] = &schemaCacheEntry{codec: codec, fetchedAt: time.Now()}
	return codec, nil
}

// GetSubjects returns a list of subjects
func (client *CachedSchemaRegistryClient) GetSubjects() ([]string, error) {
	return client.schemaRegistryClient.GetSubjects()
}

// GetVersions returns a list of all versions of a subject
func (client *CachedSchemaRegistryClient) GetVersions(subject string) ([]int, error) {
	return client.schemaRegistryClient.GetVersions(subject)
}

// GetSchemaByVersion returns the codec for a specific version of a subject
func (client *CachedSchemaRegistryClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.schemaRegistryClient.GetSchemaByVersion(subject, version)
}

// GetLatestSchema returns the highest version schema for a subject
func (client *CachedSchemaRegistryClient) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.schemaRegistryClient.GetLatestSchema(subject)
}

// CreateSubject will return and cache the id with the given codec.
// Uses double-check locking to prevent duplicate registrations under contention.
func (client *CachedSchemaRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schemaJson := codec.Schema()

	// Fast path: read lock only
	client.schemaIdCacheLock.RLock()
	entry := client.schemaIdCache[schemaJson]
	client.schemaIdCacheLock.RUnlock()
	if entry != nil && !client.isExpired(entry.fetchedAt) {
		return entry.id, nil
	}

	// Slow path: acquire write lock and double-check before registering
	client.schemaIdCacheLock.Lock()
	defer client.schemaIdCacheLock.Unlock()

	// Another goroutine may have registered while we waited for the lock
	if entry = client.schemaIdCache[schemaJson]; entry != nil && !client.isExpired(entry.fetchedAt) {
		return entry.id, nil
	}

	id, err := client.schemaRegistryClient.CreateSubject(subject, codec)
	if err != nil {
		return 0, err
	}
	client.schemaIdCache[schemaJson] = &idCacheEntry{id: id, fetchedAt: time.Now()}
	return id, nil
}

// IsSchemaRegistered checks if a specific codec is already registered to a subject
func (client *CachedSchemaRegistryClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	return client.schemaRegistryClient.IsSchemaRegistered(subject, codec)
}

// DeleteSubject deletes the subject, should only be used in development
func (client *CachedSchemaRegistryClient) DeleteSubject(subject string) error {
	return client.schemaRegistryClient.DeleteSubject(subject)
}

// DeleteVersion deletes the a specific version of a subject, should only be used in development.
func (client *CachedSchemaRegistryClient) DeleteVersion(subject string, version int) error {
	return client.schemaRegistryClient.DeleteVersion(subject, version)
}
