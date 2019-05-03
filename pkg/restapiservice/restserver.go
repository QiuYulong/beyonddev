package restapiservice

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

// RESTAPIService defines the beyond restapi server.
type RESTAPIService struct {
	address string
}

// NewRESTAPIService create new restapi server.
func NewRESTAPIService(address string) *RESTAPIService {
	return &RESTAPIService{
		address: address,
	}
}

// Run beyond restapi server.
func (r *RESTAPIService) Run(wg *sync.WaitGroup) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	srv := &http.Server{Addr: r.address} // nil Handler will use DefaultServeMux.
	log.Printf("start restapi service on %s", r.address)
	go func() {
		// register handler to DefaultServeMux.
		http.HandleFunc("/", defaultHandler)
		http.HandleFunc("/status", statusHandler)
		err := srv.ListenAndServe() // blocking call.
		if err != nil {
			log.Fatalf("restapi service ListenAndServer failed: %v", err)
		}
	}()
	sig := <-sigs
	log.Printf("signal %v received, shutting down restapi service", sig)
	if err := srv.Shutdown(context.Background()); err != nil {
		// error from closing listeners, or context timeout.
		log.Printf("restapi service shutdown failed: %v", err)
	}
	wg.Done()
}

func defaultHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello, welcome to beyond\n")
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	msg := "start_time: " + time.Now().String() + "\n"
	fmt.Fprintf(w, msg)
}
