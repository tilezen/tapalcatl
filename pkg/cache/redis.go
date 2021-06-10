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

func (m *redisCache) GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error) {
	key := buildKey(req)

	item, err := m.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// Cache miss
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting from redis: %w", err)
	}

	response, err := unmarshallData(item)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling from redis: %w", err)
	}

	return response, nil
}

func (m *redisCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error {
	key := buildKey(req)

	marshalled, err := marshallData(resp)
	if err != nil {
		return fmt.Errorf("error marshalling to redis: %w", err)
	}

	err = m.client.Set(ctx, key, marshalled, 0).Err()
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
