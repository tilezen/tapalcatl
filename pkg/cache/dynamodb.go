package cache

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/tilezen/tapalcatl/pkg/handler"
)

type dynamoCache struct {
	client dynamodbiface.DynamoDBAPI
}

func (d dynamoCache) GetTile(req *handler.ParseResult) (*handler.VectorTileResponseData, error) {
	panic("implement me")
}

func (d dynamoCache) SetTile(req *handler.ParseResult, resp *handler.VectorTileResponseData) {
	panic("implement me")
}

func NewDynamoDBCache(client dynamodbiface.DynamoDBAPI, tableARN string) *dynamoCache {
	return &dynamoCache{
		client: client,
	}
}
