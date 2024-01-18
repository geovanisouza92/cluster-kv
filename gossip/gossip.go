package gossip

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/mdns"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// connectCluster connects to a Serf cluster and returns a function that can be
// used to send user events to the cluster. It takes a logger, a stop channel,
// a service name, and a receive function as parameters. The logger is used to
// log events and errors. The stop channel is used to signal when the function
// should stop running. The service name is used to identify the service in the
// network. The receive function is called whenever a user event is received
// from the cluster. The returned function can be used to send user events to
// the cluster.
func connectCluster(
	appLogger *zap.Logger,
	stopCh <-chan struct{},
	serviceName string,
	receive func(string, []byte),
	clusterOpts ...func(*serf.Config),
) (send func(string, []byte)) {
	serviceName = fmt.Sprintf("_%s._tcp", serviceName)
	eventsCh := make(chan serf.Event, 256) // arbitrary buffer size
	cluster, leave := prepareCluster(appLogger, eventsCh, clusterOpts...)
	silence := advertisePeer(appLogger, serviceName, int(cluster.LocalMember().Port))
	peers := discoverPeers(appLogger, serviceName)

	if len(peers) > 0 {
		appLogger.Debug("joining cluster", zap.Strings("peers", peers))
		if _, err := cluster.Join(peers, true); err != nil {
			appLogger.Error("failed to join cluster", zap.Error(err), zap.Strings("peers", peers))
		}
	}

	handleQuery := func(appLogger *zap.Logger, cluster *serf.Serf, q *serf.Query) {
		appLogger.Debug("query received", zap.String("name", q.Name), zap.ByteString("payload", q.Payload))
		// no-op
	}

	handleEvent := func(appLogger *zap.Logger, cluster *serf.Serf, e serf.UserEvent) {
		appLogger.Debug("event received", zap.String("name", e.Name), zap.ByteString("payload", e.Payload))
		receive(e.Name, e.Payload)
	}

	go func() {
		defer leave()
		defer silence()

		for {
			select {
			case event := <-eventsCh:
				switch event.EventType() {
				case serf.EventQuery:
					handleQuery(appLogger, cluster, event.(*serf.Query))
				case serf.EventUser:
					handleEvent(appLogger, cluster, event.(serf.UserEvent))
				}
			case <-stopCh:
				return
			}
		}
	}()

	return func(name string, payload []byte) {
		appLogger.Debug("sending event", zap.String("name", name), zap.ByteString("payload", payload))
		if err := cluster.UserEvent(name, payload, false); err != nil {
			appLogger.Error("failed to send event", zap.Error(err), zap.String("name", name), zap.ByteString("payload", payload))
		}
	}
}

// prepareCluster sets up a Serf cluster with a given configuration. It returns
// a reference to the cluster and a cleanup function that leaves the cluster and
// closes the events channel. If the cluster creation fails, it logs a fatal
// error and the application will terminate.
func prepareCluster(appLogger *zap.Logger, eventsCh chan<- serf.Event, clusterOpts ...func(*serf.Config)) (*serf.Serf, func()) {
	// Create a standard logger that logs to the application logger. It skips
	// 3 frames to get to the correct caller (1 for this, 2 for "log" package).
	logger := log.New(&loggerWriter{appLogger.WithOptions(zap.AddCallerSkip(3))}, "", 0)

	serfConfig := serf.DefaultConfig()
	serfConfig.Logger = logger
	serfConfig.MemberlistConfig.Logger = serfConfig.Logger
	serfConfig.EventCh = eventsCh
	for _, opt := range clusterOpts { // used for testing
		opt(serfConfig)
	}

	appLogger.Debug("creating cluster")
	cluster, err := serf.Create(serfConfig)
	if err != nil {
		appLogger.Fatal("failed to create cluster", zap.Error(err))
	}

	return cluster, func() {
		appLogger.Debug("leaving cluster")
		if err := cluster.Leave(); err != nil {
			appLogger.Error("failed to leave cluster", zap.Error(err))
		}
		close(eventsCh)
	}
}

// advertisePeer sets up an mDNS server to advertise the peer's presence on the
// network. It returns a cleanup function that shuts down the mDNS server.
// If any operation (getting the hostname, creating the mDNS service or server)
// fails, it logs a fatal error and the application will terminate.
func advertisePeer(appLogger *zap.Logger, serviceName string, servicePort int) func() {
	hostName, err := os.Hostname() // Inside K8s it's the pod name
	if err != nil {
		appLogger.Fatal("failed to get hostname", zap.Error(err))
	}

	// mDNS uses the "log" package, so we need to redirect it to appLogger
	restore := zap.RedirectStdLog(appLogger)

	appLogger.Debug("advertising peer", zap.String("service", serviceName), zap.Int("port", servicePort), zap.String("host", hostName))
	svc, err := mdns.NewMDNSService(hostName, serviceName, "", "", servicePort, nil, nil)
	if err != nil {
		appLogger.Fatal("failed to create mdns service", zap.Error(err))
	}

	advertise, err := mdns.NewServer(&mdns.Config{Zone: svc})
	if err != nil {
		appLogger.Fatal("failed to create mdns server", zap.Error(err))
	}

	return func() {
		appLogger.Debug("shutting down mdns server")
		if err := advertise.Shutdown(); err != nil {
			appLogger.Error("failed to shutdown mdns server", zap.Error(err))
		}
		restore()
	}
}

// discoverPeers uses mDNS to discover peers in the network and attempts to
// join them. It logs any errors encountered during the process.
func discoverPeers(appLogger *zap.Logger, serviceName string) []string {
	appLogger.Debug("discovering peers", zap.String("service", serviceName))
	peers := []string{}

	discoverCh := make(chan *mdns.ServiceEntry, 1)
	discoverFinished := make(chan struct{})
	go func() {
		defer close(discoverFinished)
		for entry := range discoverCh {
			peers = append(peers, fmt.Sprintf("%s:%d", entry.AddrV4, entry.Port))
		}
	}()

	params := mdns.DefaultParams(serviceName)
	params.Entries = discoverCh
	params.DisableIPv6 = true // it was causing errors in dev and unlikely to be used in production
	if err := mdns.Query(params); err != nil {
		appLogger.Error("failed to lookup mdns service", zap.Error(err))
	}
	close(discoverCh)
	<-discoverFinished
	return peers
}

// loggerWriter is an adapter to use a *zap.Logger as a io.Writer for Serf logs
type loggerWriter struct {
	l *zap.Logger
}

func (l *loggerWriter) Write(p []byte) (int, error) {
	p = bytes.TrimSpace(p)
	switch string(p[:4]) {
	case "[ERR":
		l.l.Error(string(p))
	case "[WAR":
		l.l.Warn(string(p))
	case "[INF":
		l.l.Info(string(p))
	default:
		l.l.Debug(string(p))
	}
	return len(p), nil
}
