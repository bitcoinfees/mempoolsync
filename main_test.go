package main

import (
	"flag"
	"os"
	"testing"
)

var testConfig RPCConfig

func TestMain(m *testing.M) {
	flag.StringVar(&testConfig.Addr, "addr", "localhost:8332", "bitcoin json-rpc address")
	flag.StringVar(&testConfig.User, "user", "", "json-rpc username")
	flag.StringVar(&testConfig.Password, "password", "", "json-rpc password")
	flag.Parse()

	os.Exit(m.Run())
}

func TestApp(t *testing.T) {
	go runServer("localhost:8000", testConfig)
	if err := runClient("localhost:8000", testConfig); err != nil {
		t.Fatal(err)
	}
}
