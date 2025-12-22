package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache wraps Redis client with enabled flag.
// If Redis connection fails, Enabled will be set to false.
type Cache struct {
	client  *redis.Client
	enabled bool
	ttl     time.Duration
	env     string // 執行環境 (dev/staging/prod)
}

// NewCache creates a new cache instance.
// If Redis connection fails, enabled will be set to false.
func NewCache(redisURL string, enabled bool, ttlSeconds int, env string) (*Cache, error) {
	cache := &Cache{
		enabled: false,
		ttl:     time.Duration(ttlSeconds) * time.Second,
		env:     env,
	}

	if !enabled {
		cache.logInfo("[Redis] Cache disabled (REDIS_ENABLED=false)")
		return cache, nil
	}

	if redisURL == "" {
		cache.logInfo("[Redis] Cache disabled (REDIS_URL not set)")
		return cache, nil
	}

	cache.logInfo("[Redis] Initializing cache with URL: %s, TTL: %d seconds", redisURL, ttlSeconds)

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		cache.logError("[Redis] Failed to parse Redis URL: %v", err)
		return cache, nil
	}

	client := redis.NewClient(opt)

	// 測試連線，如果失敗則將 enabled 設為 false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		cache.logError("[Redis] Connection failed: %v", err)
		_ = client.Close()
		return cache, nil
	}

	cache.client = client
	cache.enabled = true
	cache.logInfo("[Redis] Cache enabled and connected successfully")
	return cache, nil
}

// Enabled returns whether cache is enabled.
func (c *Cache) Enabled() bool {
	return c.enabled && c.client != nil
}

// logInfo 輸出資訊類日誌，prod 環境不輸出
func (c *Cache) logInfo(format string, v ...interface{}) {
	if c.env != "prod" {
		log.Printf(format, v...)
	}
}

// logError 輸出錯誤日誌，所有環境都輸出
func (c *Cache) logError(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Close closes the Redis client.
func (c *Cache) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Get retrieves a value from cache.
func (c *Cache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if !c.Enabled() {
		return false, nil
	}

	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		c.logInfo("[Redis] Cache miss: %s", key)
		return false, nil
	}
	if err != nil {
		c.logError("[Redis] Get error for key %s: %v (disabling cache)", key, err)
		// 如果讀取失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return false, nil
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		c.logError("[Redis] Unmarshal error for key %s: %v", key, err)
		return false, fmt.Errorf("unmarshal cache value: %w", err)
	}

	c.logInfo("[Redis] Cache hit: %s", key)
	return true, nil
}

// Set stores a value in cache.
func (c *Cache) Set(ctx context.Context, key string, value interface{}) error {
	if !c.Enabled() {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		c.logError("[Redis] Marshal error for key %s: %v", key, err)
		return fmt.Errorf("marshal cache value: %w", err)
	}

	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		c.logError("[Redis] Set error for key %s: %v (disabling cache)", key, err)
		// 如果寫入失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil // 不返回錯誤，讓查詢繼續進行
	}

	c.logInfo("[Redis] Cache set: %s (TTL: %v)", key, c.ttl)
	return nil
}

// Delete removes a key from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if !c.Enabled() {
		return nil
	}

	if err := c.client.Del(ctx, key).Err(); err != nil {
		c.logError("[Redis] Delete error for key %s: %v (disabling cache)", key, err)
		// 如果刪除失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil
	}

	c.logInfo("[Redis] Cache deleted: %s", key)
	return nil
}

// GenerateCacheKey generates a cache key from query parameters.
func GenerateCacheKey(prefix string, params interface{}) string {
	data, err := json.Marshal(params)
	if err != nil {
		// 如果序列化失敗，使用簡單的 key
		return fmt.Sprintf("%s:fallback", prefix)
	}

	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("%s:%s", prefix, hashStr)
}
