package cache

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/tile"
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
	key := buildVectorTileKey(req)

	item, err := m.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error getting from redis: %w", err)
	}

	if item == nil {
		return nil, nil
	}

	response, err := unmarshallVectorTileData(item)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *redisCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error {
	key := buildVectorTileKey(req)

	marshalled, err := marshallVectorTileData(resp)
	if err != nil {
		return fmt.Errorf("error marshalling to redis: %w", err)
	}

	err = m.Set(ctx, key, marshalled)
	if err != nil {
		return fmt.Errorf("error setting to redis: %w", err)
	}

	return nil
}

func (m *redisCache) GetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord) (*state.MetatileResponseData, error) {
	key := buildMetatileKey(req, metaCoord)

	item, err := m.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error getting from redis: %w", err)
	}

	if item == nil {
		return nil, nil
	}

	response, err := unmarshallMetatileData(item)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (m *redisCache) SetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord, resp *state.MetatileResponseData) error {
	key := buildMetatileKey(req, metaCoord)

	marshalled, err := marshallMetatileData(resp)
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
