package ddb

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

func receiveNotification(appLogger *zap.Logger) func(string, []byte) {
	return func(name string, payload []byte) {
		// TODO
	}
}

func dispatchNotification(appLogger *zap.Logger, send func(string, []byte)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var event dynamodbChangeEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		for _, record := range event.Records {
			// TODO: send keys
			if len(record.Dynamodb.NewImage) == 0 {
				send("delete", []byte("keys"))
			} else {
				send("update", []byte("keys"))
			}
		}
	}
}

type dynamodbChangeEvent struct {
	Records []struct {
		EventID        string `json:"eventID"`
		EventName      string `json:"eventName"`
		EventVersion   string `json:"eventVersion"`
		EventSource    string `json:"eventSource"`
		EventSourceArn string `json:"eventSourceARN"`
		AwsRegion      string `json:"awsRegion"`
		Dynamodb       struct {
			Keys           map[string]any `json:"Keys"`
			NewImage       map[string]any `json:"NewImage"`
			OldImage       map[string]any `json:"OldImage"`
			SequenceNumber string         `json:"SequenceNumber"`
			SizeBytes      int            `json:"SizeBytes"`
			StreamViewType string         `json:"StreamViewType"`
		} `json:"dynamodb"`
	} `json:"Records"`
}
