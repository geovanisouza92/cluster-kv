package main

import (
	"context"
	"io"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/mdns"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

const (
	clusterGet = "GET"
	clusterDel = "DEL"
)

var log *logrus.Logger

func main() {
	log = logrus.New()
	log.Formatter = new(logrus.JSONFormatter)

	evCh := make(chan serf.Event, 64)
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
		if err := txn.Set([]byte(key), val); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		if err := txn.Commit(); err != nil {
			log.WithField("key", key).Error(err)
			return err
		}
		return cluster.UserEvent(clusterDel, key, false)
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
					val, err := get(q.Payload)
					if err != nil {
						log.WithField("key", q.Payload).Error(err)
						continue
					}
					if val != nil {
						q.Respond(val)
					}
				}
			case serf.EventUser:
				u, ok := ev.(*serf.UserEvent)
				if !ok {
					log.Error("invalid user event")
					continue
				}

				if u.Name == clusterDel {
					del(u.Payload)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := []byte(r.URL.Path[1:])
		if r.Method == http.MethodGet {
			log.Println("READ:", string(key))
			val, err := get(key)
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
				for res := range queryRes.ResponseCh() {
					val = res.Payload
					queryRes.Close()
				}

				if val == nil {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}

				if err := set(key, val); err != nil {
					log.WithField("key", key).Error(err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
			}
			w.Write(val)
		} else if r.Method == http.MethodPost {
			log.Println("WRITE:", string(key))
			value, err := io.ReadAll(r.Body)
			if err != nil {
				log.WithField("key", key).Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := set(key, value); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})

	srv := &http.Server{
		Addr:    ":9876",
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

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
