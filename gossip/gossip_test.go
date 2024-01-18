package gossip

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestConnectCluster(t *testing.T) {
	// The CI environment doesn't allow port binding, so we skip this test.
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI environment")
	}

	// Arrange
	testLogger := zaptest.NewLogger(t)
	stopCh := make(chan struct{})
	serviceName := "test-service"

	var wg sync.WaitGroup
	wg.Add(2)

	var aliceDidReceive, bobDidReceive bool
	sendAlice := connectCluster(
		testLogger.With(zap.String("actor", "alice")), // tag logs with actor
		stopCh,
		serviceName,
		func(name string, payload []byte) {
			// both receive all messages, so we check the sender
			if strings.Contains(string(payload), "Bob") {
				aliceDidReceive = true
				wg.Done()
			}
		},
		func(serfConfig *serf.Config) {
			// Serf doesn't like multiple nodes on the same host, so we need to
			// make sure that Alice and Bob are on different ports and names.
			serfConfig.NodeName = "alice"
			serfConfig.MemberlistConfig.BindPort = 0
			serfConfig.MemberlistConfig.GossipInterval = 20 * time.Millisecond
		},
	)
	sendBob := connectCluster(
		testLogger.With(zap.String("actor", "bob")), // tag logs with actor
		stopCh,
		serviceName,
		func(name string, payload []byte) {
			// both receive all messages, so we check the sender
			if strings.Contains(string(payload), "Alice") {
				bobDidReceive = true
				wg.Done()
			}
		},
		func(serfConfig *serf.Config) {
			// Serf doesn't like multiple nodes on the same host, so we need to
			// make sure that Alice and Bob are on different ports and names.
			serfConfig.NodeName = "bob"
			serfConfig.MemberlistConfig.BindPort = 0
			serfConfig.MemberlistConfig.GossipInterval = 20 * time.Millisecond
		},
	)

	// Act
	sendAlice("from-alice", []byte("Alice's message"))
	sendBob("from-bob", []byte("Bob's message"))
	wg.Wait()     // wait for both Alice and Bob to receive a message
	close(stopCh) // stop the cluster

	// Assert
	assert.True(t, aliceDidReceive, "Alice should have received a message from Bob")
	assert.True(t, bobDidReceive, "Bob should have received a message from Alice")
}
