package clusterkv

import (
	"errors"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/mdns"
	"github.com/hashicorp/serf/serf"
)

type Config struct {
	DataDir string
}

func DefaultConfig() Config {
	return Config{
		DataDir: "/tmp/clusterkv",
	}
}

type clusterKV struct {
	c *cluster
	s *store
}

func New(cfg Config, opts ...Option) (*clusterKV, error) {
	c, err := newCluster()
	if err != nil {
		return nil, err
	}

	s, err := newStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	k := &clusterKV{c, s}
	for _, opt := range opts {
		if err := opt(k); err != nil {
			return nil, err
		}
	}

	return k, nil
}

func (c *clusterKV) Close() error {
	errs := make([]error, 1)
	if err := c.s.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := c.c.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 1 {
		return errors.Join(errs...)
	}
	return nil
}

type cluster struct {
	s  *serf.Serf
	ch chan serf.Event
}

func newCluster() (*cluster, error) {
	ch := make(chan serf.Event, 1)

	cfg := serf.DefaultConfig()
	cfg.EventCh = ch

	s, err := serf.Create(cfg)
	if err != nil {
		return nil, err
	}

	c := &cluster{s, ch}
	go c.dispatchEvents()

	return c, nil
}

func (c *cluster) Close() error {
	close(c.ch)
	errs := make([]error, 1)
	if err := c.s.Leave(); err != nil {
		errs = append(errs, err)
	}
	if err := c.s.Shutdown(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 1 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *cluster) dispatchEvents() {
	for range c.ch {
		// TODO
	}
}

type store struct {
	db *badger.DB
}

func newStore(dir string) (*store, error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}
	return &store{db}, nil
}

func (s *store) Close() error {
	return s.db.Close()
}

type Option func(*clusterKV) error

func WithAutoDiscovery(svcName string, port int) Option {
	return func(c *clusterKV) error {
		host, err := os.Hostname()
		if err != nil {
			return err
		}

		svc, err := mdns.NewMDNSService(host, fmt.Sprintf("_%s._tcp", svcName), "", "", port, nil, nil)
		if err != nil {
			return err
		}

		s, err := mdns.NewServer(&mdns.Config{Zone: svc})
		if err != nil {
			return err
		}
		defer func() {
			if err := s.Shutdown(); err != nil {
				// TODO Log
				return
			}
		}()

		ch := make(chan *mdns.ServiceEntry)

		var err2 error
		go func() {
			defer close(ch)

			if err2 = mdns.Lookup(svcName, ch); err2 != nil {
				return
			}
		}()

		ips := make([]string, 1)
		for e := range ch {
			ips = append(ips, e.AddrV4.String())
		}

		if err2 != nil {
			return err2
		}

		if _, err = c.c.s.Join(ips, true); err != nil {
			return err
		}

		return nil
	}
}
