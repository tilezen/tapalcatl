package cache

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/tilezen/tapalcatl/pkg/state"
)

type redisCache struct {
	client *redis.Client
}

func (m *redisCache) Get(ctx context.Context, key string) ([]byte, error) {
	bytes, err := m.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			// Redis responds with a Nil error if there was a miss.
			return nil, nil
		}

		return nil, err
	}

	return bytes, nil
}

func (m *redisCache) Set(ctx context.Context, key string, val []byte) error {
	err := m.client.Set(ctx, key, val, 0).Err()
	if err != nil {
		return fmt.Errorf("error setting to redis: %w", err)
	}

	return nil
}

func (m *redisCache) GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error) {
	key := buildKey(req)

	item, err := m.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error getting from redis: %w", err)
	}

	if item == nil {
		return nil, nil
	}

	response, err := unmarshallData(item)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *redisCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error {
	key := buildKey(req)

	marshalled, err := marshallData(resp)
	if err != nil {
		return fmt.Errorf("error marshalling to redis: %w", err)
	}

	err = m.Set(ctx, key, marshalled)
	if err != nil {
		return fmt.Errorf("error setting to redis: %w", err)
	}

	return nil
}

func NewRedisCache(client *redis.Client) Cache {
	return &redisCache{
		client: client,
	}
}