package ddb

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestReceiveNotification(t *testing.T) {
	testLogger := zaptest.NewLogger(t)
	receive := receiveNotification(testLogger)
	receive("test", []byte("test"))
}

func TestDispatchNotification(t *testing.T) {
	// Arrange
	testLogger := zaptest.NewLogger(t)

	var updateCount, deleteCount int
	handler := dispatchNotification(testLogger, func(name string, payload []byte) {
		if name == "update" {
			updateCount++
		} else if name == "delete" {
			deleteCount++
		}
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	b := new(bytes.Buffer)
	b.WriteString(dynamodbChangeEventJSON)

	req, err := http.NewRequest("POST", ts.URL, b)
	assert.NoError(t, err)

	// Act
	res, err := ts.Client().Do(req)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, 2, updateCount, "send func should have received 2 update events")
	assert.Equal(t, 1, deleteCount, "send func should have received 1 delete event")
}

const dynamodbChangeEventJSON = `{
	"Records":[
		 {
				"eventID":"1",
				"eventName":"INSERT",
				"eventVersion":"1.0",
				"eventSource":"aws:dynamodb",
				"awsRegion":"us-east-1",
				"dynamodb":{
					 "Keys":{
							"Id":{
								 "N":"101"
							}
					 },
					 "NewImage":{
							"Message":{
								 "S":"New item!"
							},
							"Id":{
								 "N":"101"
							}
					 },
					 "SequenceNumber":"111",
					 "SizeBytes":26,
					 "StreamViewType":"NEW_AND_OLD_IMAGES"
				},
				"eventSourceARN":"stream-ARN"
		 },
		 {
				"eventID":"2",
				"eventName":"MODIFY",
				"eventVersion":"1.0",
				"eventSource":"aws:dynamodb",
				"awsRegion":"us-east-1",
				"dynamodb":{
					 "Keys":{
							"Id":{
								 "N":"101"
							}
					 },
					 "NewImage":{
							"Message":{
								 "S":"This item has changed"
							},
							"Id":{
								 "N":"101"
							}
					 },
					 "OldImage":{
							"Message":{
								 "S":"New item!"
							},
							"Id":{
								 "N":"101"
							}
					 },
					 "SequenceNumber":"222",
					 "SizeBytes":59,
					 "StreamViewType":"NEW_AND_OLD_IMAGES"
				},
				"eventSourceARN":"stream-ARN"
		 },
		 {
				"eventID":"3",
				"eventName":"REMOVE",
				"eventVersion":"1.0",
				"eventSource":"aws:dynamodb",
				"awsRegion":"us-east-1",
				"dynamodb":{
					 "Keys":{
							"Id":{
								 "N":"101"
							}
					 },
					 "OldImage":{
							"Message":{
								 "S":"This item has changed"
							},
							"Id":{
								 "N":"101"
							}
					 },
					 "SequenceNumber":"333",
					 "SizeBytes":38,
					 "StreamViewType":"NEW_AND_OLD_IMAGES"
				},
				"eventSourceARN":"stream-ARN"
		 }
	]
}`
