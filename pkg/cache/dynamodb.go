package cache

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/tilezen/tapalcatl/pkg/state"
)

type dynamoCache struct {
	client    dynamodbiface.DynamoDBAPI
	tableName string
}

func (d dynamoCache) GetTile(req *state.ParseResult) (*state.VectorTileResponseData, error) {
	key := buildKey(req)

	dynamoItem, err := d.client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"p": {
				S: aws.String(key),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error calling GetItem: %w", err)
	}

	if dynamoItem.Item == nil {
		return nil, nil
	}

	responseData := state.VectorTileResponseData{}
	err = dynamodbattribute.UnmarshalMap(dynamoItem.Item, &responseData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling cached item: %w", err)
	}

	return &responseData, nil
}

func (d dynamoCache) SetTile(req *state.ParseResult, resp *state.VectorTileResponseData) error {
	key := buildKey(req)

	dynamoItem, err := dynamodbattribute.MarshalMap(resp)
	if err != nil {
		return fmt.Errorf("error marshalling Dynamo item: %w", err)
	}

	dynamoItem["p"] = &dynamodb.AttributeValue{S: aws.String(key)}

	_, err = d.client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      dynamoItem,
	})
	if err != nil {
		return fmt.Errorf("error calling PutItem: %w", err)
	}

	return nil
}

func NewDynamoDBCache(client dynamodbiface.DynamoDBAPI, tableName string) *dynamoCache {
	return &dynamoCache{
		client:    client,
		tableName: tableName,
	}
}
