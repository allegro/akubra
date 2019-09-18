package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/allegro/akubra/log"
)

func init() {
	go func() {
		log.Fatal(http.ListenAndServe(":6001", http.DefaultServeMux))
	}()
}
