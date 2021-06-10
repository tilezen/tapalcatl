package cache

import (
	"encoding/json"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/tilezen/tapalcatl/pkg/state"
)

type memcacheClient struct {
	client *memcache.Client
}

func (m *memcacheClient) GetTile(req *state.ParseResult) (*state.VectorTileResponseData, error) {
	key := buildKey(req)

	item, err := m.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil, nil
		}

		return nil, fmt.Errorf("error getting from memcache: %w", err)
	}

	response := state.VectorTileResponseData{}
	err = json.Unmarshal(item.Value, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling from memcache: %w", err)
	}

	return &response, nil
}

func (m *memcacheClient) SetTile(req *state.ParseResult, resp *state.VectorTileResponseData) error {
	key := buildKey(req)

	marshalled, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("error marshalling to memcache: %w", err)
	}

	err = m.client.Set(&memcache.Item{
		Key:   key,
		Value: marshalled,
	})
	if err != nil {
		return fmt.Errorf("error setting to memcache: %w", err)
	}

	return nil
}

func NewMemcacheCache(client *memcache.Client) Cache {
	return &memcacheClient{
		client: client,
	}
}
