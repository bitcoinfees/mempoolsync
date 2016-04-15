package main

import (
	"encoding/hex"
	"testing"
)

func TestRPC(t *testing.T) {
	// Test getBlockCount
	height, err := getBlockCount(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("getBlockCount:", height)

	// Test getBlockHash
	hash, err := getBlockHash(height, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("getBlockHash:", hex.EncodeToString(hash))

	// Test getRawMempool
	mempool, err := getRawMempool(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("getRawMempool: %d entries", len(mempool))

	// Test getRawTransaction
	var txid string
	for txid = range mempool {
		// Get random txid
		break
	}
	tx, err := getRawTransaction(txid, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("getRawTransaction: %s: %d bytes", txid, len(tx))

	// Test sendRawTransaction
	if err := sendRawTransaction(tx, testConfig); err != nil {
		t.Fatal(err)
	}
	t.Log("sendRawTransaction successful.")
}
