package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/geovanisouza92/global-kv/clusterkv"
	"github.com/hashicorp/mdns"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	clusterGet = "GET"
	clusterDel = "DEL"
	maxKeySize = 1024
	maxValSize = 1024
)

var log *logrus.Logger

// getKeyRequest is used when a node don't have the key in its local store and
// needs to ask the cluster for it. Maximum key size is 1024 bytes.
type getKeyRequest struct {
	Key []byte
}

// getKeyResponse is returned by a node that has the key in its local store.
// The Value is compressed. Maximum compressed value size is 1024 bytes.
type getKeyResponse struct {
	Type    getKeyResponseType
	Payload []byte
}

type getKeyResponseType byte

const (
	getKeyResponseTypeFound getKeyResponseType = iota
	getKeyResponseTypeRedirect
)

// delKeyRequest is used to notify the cluster that a key has been written and
// all nodes should delete it from their local store if it exists. Maximum key
// size is 1024 bytes.
type delKeyRequest struct {
	SourceNode string
	Key        []byte
}

func main() {
	k, err := clusterkv.New(clusterkv.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer k.Close()

	log = logrus.New()
	log.Formatter = new(logrus.JSONFormatter)

	evCh := make(chan serf.Event, 1)
	defer close(evCh)

	cfg := serf.DefaultConfig()
	cfg.EventCh = evCh
	cfg.Logger = stdlog.New(log.WithField("component", "serf").Writer(), "", 0)
	cfg.MemberlistConfig.Logger = cfg.Logger
	cluster, err := serf.Create(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := cluster.Leave(); err != nil {
			log.Printf("failed to leave cluster: %v\n", err)
		}
	}()

	host, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	svc, err := mdns.NewMDNSService(host, "_global_kv._tcp", "", "", 9876, nil, []string{"..."})
	if err != nil {
		log.Fatal(err)
	}
	server, err := mdns.NewServer(&mdns.Config{Zone: svc})
	if err != nil {
		log.Fatal(err)
	}

	entriesCh := make(chan *mdns.ServiceEntry, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		members := []string{}
		for entry := range entriesCh {
			log.Printf("found entry: %v\n", entry)
			members = append(members, entry.AddrV4.String())
		}

		if len(members) > 0 {
			log.Printf("joining nodes: %v\n", members)
			if _, err = cluster.Join(members, false); err != nil {
				log.Fatal(err)
			}
		}
	}()

	if err := mdns.Lookup("_global_kv._tcp", entriesCh); err != nil {
		log.Fatal(err)
	}
	close(entriesCh)
	wg.Wait()

	defer func() {
		if err := server.Shutdown(); err != nil {
			log.Printf("failed to shutdown mdns server: %v\n", err)
		}
	}()

	dir, fn3 := getDataDir()
	defer fn3()

	dbOpts := badger.DefaultOptions(dir).WithLogger(log.WithField("component", "badger"))
	db, err := badger.Open(dbOpts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db: %v\n", err)
		}
	}()

	get := func(key []byte) (out []byte, err error) {
		err = db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil
				}

				log.WithField("key", key).Error(err)
				return err
			}

			out, err = item.ValueCopy(nil)
			return err
		})
		return
	}

	set := func(key, val []byte) error {
		txn := db.NewTransaction(true)
		if err := txn.Set(key, val); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		if err := txn.Commit(); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		return nil
	}

	del := func(key []byte) error {
		var skip bool
		db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			skip = err == badger.ErrKeyNotFound
			return nil
		})
		if skip {
			return nil
		}

		txn := db.NewTransaction(true)
		if err := txn.Delete(key); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		if err := txn.Commit(); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		return nil
	}

	go func() {
		for ev := range evCh {
			switch ev.EventType() {
			case serf.EventQuery:
				q, ok := ev.(*serf.Query)
				if !ok {
					log.Error("invalid query event")
					continue
				}

				if q.Name == clusterGet {
					var req getKeyRequest
					if err := msgpack.Unmarshal(q.Payload, &req); err != nil {
						log.WithField("payload", q.Payload).Error(err)
						continue
					}
					val, err := get(req.Key /*, decompress: false */)
					if err != nil {
						log.WithField("key", req.Key).Error(err)
						continue
					}
					if val != nil {
						var res getKeyResponse
						if len(val) < maxValSize {
							res = getKeyResponse{Payload: val}
						} else {
							res = getKeyResponse{Type: getKeyResponseTypeRedirect, Payload: []byte(cluster.LocalMember().Name)}
						}

						out, err := msgpack.Marshal(res)
						if err != nil {
							log.WithField("key", req.Key).Error(err)
							continue
						}
						q.Respond(out)
					}
				}
			case serf.EventUser:
				u, ok := ev.(serf.UserEvent)
				if !ok {
					log.Errorf("invalid user event, type(%T): %v", ev, ev)
					continue
				}

				var req delKeyRequest
				if err := msgpack.Unmarshal(u.Payload, &req); err != nil {
					log.WithField("payload", u.Payload).Error(err)
					continue
				}

				if req.SourceNode == cluster.LocalMember().Name {
					log.Debug("skipping delete request from self")
					continue
				}

				if u.Name == clusterDel {
					del(req.Key)
				}
			}
		}
	}()

	keyHashers := sync.Pool{
		New: func() any {
			return sha256.New()
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := []byte(r.URL.Path)
		if len(key) == 0 {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		if len(key) >= maxKeySize { // If the key length is too big, digest it with SHA256
			hash := keyHashers.Get().(hash.Hash)
			hash.Reset()
			hash.Write(key)
			key = hash.Sum(nil)
			keyHashers.Put(hash)
		}

		if r.Method == http.MethodGet {
			log.Println("READ:", string(key))
			val, err := get(key /*, decompress: true */)
			if err != nil {
				log.WithField("key", key).Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if val == nil {
				queryRes, err := cluster.Query(clusterGet, key, cluster.DefaultQueryParams())
				if err != nil {
					log.WithField("key", key).Error(err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				var getRes getKeyResponse
				for res := range queryRes.ResponseCh() {
					// Decode val
					if err := msgpack.Unmarshal(res.Payload, &getRes); err != nil {
						log.WithField("key", key).Error(err)
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					queryRes.Close()
				}

				if getRes.Type == getKeyResponseTypeRedirect {
					http.Redirect(w, r, fmt.Sprintf("http://%s%s", string(getRes.Payload), r.URL.Path), http.StatusTemporaryRedirect)
					return
				}

				if getRes.Payload == nil {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}

				if err := set(key, getRes.Payload); err != nil {
					log.WithField("key", key).Error(err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				val = getRes.Payload
			}

			// TODO Decompress val
			w.Write(val)
		} else if r.Method == http.MethodPost {
			log.Println("WRITE:", string(key))
			value, err := io.ReadAll(r.Body)
			if err != nil {
				log.WithField("key", key).Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// TODO If value length is too big, compress it

			if err := set(key, value); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			req := delKeyRequest{SourceNode: cluster.LocalMember().Name, Key: key}
			out, err := msgpack.Marshal(req)
			if err != nil {
				log.WithField("key", key).Error(err)
				return
			}
			if err := cluster.UserEvent(clusterDel, out, false); err != nil {
				log.WithField("key", key).Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})
	// mux.Handle("/notify",
	// 	dispatchNotification(appLogger,
	// 		connectCluster(appLogger, interrupt.Interrupted(), "seawall",
	// 			receiveNotification(appLogger),
	// 		),
	// 	),
	// )

	srv := &http.Server{
		Addr:    ":9876",
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// c := newCluster(nil)
	// s := newStore(nil)
	// ctl := newController(c, s)
	// srv.Handler = ctl

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	<-s
	srv.Shutdown(context.Background())

	log.Println("exiting")
}

func getDataDir() (string, func()) {
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		dir, err := os.MkdirTemp("", "global-kv")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("temp dir: %s\n", dir)
		return dir, func() {
			if err := os.RemoveAll(dir); err != nil {
				log.Printf("failed to remove temp dir %s: %v\n", dir, err)
			}
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	dir := path.Join("/data", hostname)
	return dir, func() {}
}

// ask will send a query to the cluster to get the value of the key. If the key
// is not found, it will return nil.
func ask(key Key) Value {
	return nil
}

// purge will notify the cluster to delete a key.
func purge(key Key) {
}

type Key []byte
type Value []byte

type cluster struct {
	s  *serf.Serf
	ch chan serf.Event
	h  map[string]func(Key)
}

func newCluster(cd clusterDiscovery) *cluster {
	cfg := serf.DefaultConfig()
	ch := make(chan serf.Event, 1)
	cfg.EventCh = ch
	s, _ := serf.Create(cfg)
	c := &cluster{s: s, ch: ch, h: make(map[string]func(Key))}
	go c.handleEvents()
	go c.discover(cd)
	return c
}

func (c *cluster) Close() error {
	if err := c.s.Leave(); err != nil {
		return err
	}
	return c.s.Shutdown()
}

func (c *cluster) OnPurge(fn func(Key)) {
	c.h[clusterDel] = fn
}

func (c *cluster) Query(key Key) Value {
	return nil
}

func (c *cluster) Purge(key Key) {
}

func (c *cluster) handleEvents() {
	for ev := range c.ch {
		switch ev.EventType() {
		case serf.EventQuery:
			//
		case serf.EventUser:
			//
		}
	}
}

func (c *cluster) discover(cd clusterDiscovery) {
	m, _ := cd.FetchMembers()
	c.s.Join(m, true)
}

type clusterDiscovery interface {
	FetchMembers() ([]string, error)
}

type store struct {
	db *badger.DB
}

func newStore(db *badger.DB) *store {
	return &store{db}
}

func (s *store) Get(key Key) Value {
	var val Value
	s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		b, _ := item.ValueCopy(nil)
		val = Value(b)
		return nil
	})
	return val
}

func (s *store) Set(key Key, val Value) {
	s.db.Update(func(txn *badger.Txn) error {
		txn.Set(key, val)
		return nil
	})
}

func (s *store) Del(key Key) {
	txn := s.db.NewTransaction(true)
	txn.Delete(key)
	txn.Commit()
}

type controller struct {
	c *cluster
	s *store

	k, v sync.Pool
}

func newController(c *cluster, s *store) *controller {
	c.OnPurge(func(key Key) {
		s.Del(key)
	})
	return &controller{
		c: c,
		s: s,
		k: sync.Pool{
			New: func() any {
				return sha256.New()
			},
		},
		v: sync.Pool{
			New: func() any {
				var b bytes.Buffer
				return &b
			},
		},
	}
}

func (c *controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		c.read(w, r)
	case http.MethodPost:
		c.write(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *controller) read(w http.ResponseWriter, r *http.Request) {
	key := c.keyOf([]byte(r.URL.Path))
	val := c.s.Get(key)
	if val == nil {
		val = c.c.Query(key)
		if val == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		c.s.Set(key, val)
	}
	c.respond(w, val)
}

func (c *controller) write(w http.ResponseWriter, r *http.Request) {
	key := c.keyOf([]byte(r.URL.Path))
	val := c.valueOf(r.Body)
	c.s.Set(key, val)
	c.c.Purge(key)
	w.WriteHeader(http.StatusCreated)
}

// keyOf takes an input and returns a normalized key with maxKeySize length. If
// required, it will digest the input with SHA256 to have a predictable length.
func (c *controller) keyOf(input []byte) Key {
	hash := c.k.Get().(hash.Hash)
	defer c.k.Put(hash)
	hash.Reset()
	hash.Write(input)
	key := hash.Sum(nil)
	return key
}

// valueOf takes an io.Reader and returns the compressed value suitable for
// storage and network transmission.
func (c *controller) valueOf(r io.Reader) Value {
	b := c.v.Get().(*bytes.Buffer)
	defer c.v.Put(b)
	b.Reset()

	z := zlib.NewWriter(b)
	if _, err := io.Copy(z, r); err != nil {
		return nil // TODO
	}
	if err := z.Close(); err != nil {
		return nil // TODO
	}

	return b.Bytes()
}

// respond will write the value to the writer. If the value is compressed, it
// will be decompressed before writing.
func (c *controller) respond(w io.Writer, val Value) {
	rc, err := zlib.NewReader(bytes.NewReader(val))
	if err != nil {
		return // TODO
	}
	defer rc.Close()

	if _, err := io.Copy(w, rc); err != nil {
		return // TODO
	}
}
