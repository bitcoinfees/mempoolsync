package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"log"
	"net"
)

type MempoolEntry struct {
	Depends []string
}

type GobConn struct {
	enc  *gob.Encoder
	dec  *gob.Decoder
	conn net.Conn
}

func (g *GobConn) CheckTip(tip []byte) error {
	var rTip []byte
	e := g.EncodeAsync(tip)
	d := g.DecodeAsync(&rTip)
	if err := <-e; err != nil {
		return err
	}
	if err := <-d; err != nil {
		return err
	}

	if !bytes.Equal(tip, rTip) {
		return errors.New("Local and remote tip are different, try again later.")
	}
	return nil
}

func (g *GobConn) EncodeAsync(e interface{}) <-chan error {
	ec := make(chan error)
	go func() {
		err := g.enc.Encode(e)
		ec <- err
		close(ec)
	}()
	return ec
}

func (g *GobConn) DecodeAsync(e interface{}) <-chan error {
	ec := make(chan error)
	go func() {
		err := g.dec.Decode(e)
		ec <- err
		close(ec)
	}()
	return ec
}

func (g *GobConn) Close() error {
	return g.conn.Close()
}

type GobListener struct {
	l net.Listener
}

func (l *GobListener) Accept() (*GobConn, error) {
	conn, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	g := &GobConn{
		enc:  gob.NewEncoder(conn),
		dec:  gob.NewDecoder(conn),
		conn: conn,
	}
	return g, nil
}

func main() {
	var (
		rpcUser, rpcPassword string
		rpcAddr              string
		dialAddr             string
		listenAddr           string
	)
	flag.StringVar(&rpcUser, "rpcuser", "", "bitcoind RPC username")
	flag.StringVar(&rpcPassword, "rpcpassword", "", "bitcoind RPC password")
	flag.StringVar(&rpcAddr, "rpcaddr", "localhost:8332", "bitcoind RPC address as <host:port>")
	flag.StringVar(&dialAddr, "c", "", "Client mode; connect to server at <host:port>.")
	flag.StringVar(&listenAddr, "l", "",
		"Server mode; listen for client connections on <host:port>.")
	flag.Parse()

	cfg := RPCConfig{
		Addr:     rpcAddr,
		User:     rpcUser,
		Password: rpcPassword,
	}

	if listenAddr != "" {
		// Server mode
		l, err := setupServer(listenAddr)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Listening on", listenAddr)
		for {
			g, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			log.Println("Connected from", g.conn.RemoteAddr())
			// Only accept one connection at a time.
			if err := handleConn(g, cfg); err != nil {
				log.Fatal(err)
			}
		}
	} else if dialAddr != "" {
		// Client mode
		g, err := setupClient(dialAddr)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Connected to", dialAddr)
		if err := handleConn(g, cfg); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("Need to specify either -c or -l.")
	}
}

func handleConn(g *GobConn, cfg RPCConfig) error {
	defer g.Close()
	const finished = 0x5A

	height, err := getBlockCount(cfg)
	if err != nil {
		return err
	}
	tip, err := getBlockHash(height, cfg)
	if err != nil {
		return err
	}
	if err := g.CheckTip(tip); err != nil {
		return err
	}
	log.Println("Tip check OK.")

	mempool, err := getRawMempool(cfg)
	if err != nil {
		return err
	}
	txids := make([]string, 0, len(mempool))
	for txid := range mempool {
		txids = append(txids, txid)
	}
	var rTxids []string

	// Send and receive the txids
	e := g.EncodeAsync(txids)
	d := g.DecodeAsync(&rTxids)
	if err := <-e; err != nil {
		return err
	}
	if err := <-d; err != nil {
		return err
	}
	log.Println("Txid list exchange complete.")

	// Get the list of txids to send
	rTxidMap := make(map[string]bool)
	for _, txid := range rTxids {
		rTxidMap[txid] = true
	}
	remoteHas := func(txid string) bool {
		return rTxidMap[txid]
	}
	sendList := getSendList(mempool, remoteHas)
	log.Printf("Sending %d of %d txs.", len(sendList), len(mempool))

	// Send and receive the Number of raw txs to expect
	var rLen int
	e = g.EncodeAsync(len(sendList))
	d = g.DecodeAsync(&rLen)
	if err := <-e; err != nil {
		return err
	}
	if err := <-d; err != nil {
		return err
	}
	log.Printf("Expecting %d remote txs..", rLen)

	// Now do the sending / receiving
	localTxs := make(chan []byte, 10)
	remoteTxs := make(chan []byte, 10)
	done := make(chan struct{})
	defer close(done)
	e = encodeTxs(localTxs, g)
	d = decodeTxs(remoteTxs, rLen, g)
	getErr := getTxs(localTxs, sendList, cfg, done)
	sendErr := sendTxs(remoteTxs, cfg)

	var numClosed int
	numAdded := rLen
	for {
		select {
		case err := <-getErr:
			if err != nil {
				log.Println("getrawtransaction error:", err)
			} else {
				numClosed++
				getErr = nil
			}
		case err := <-sendErr:
			if err != nil {
				log.Println("sendrawtransaction error:", err)
				numAdded--
			} else {
				numClosed++
				sendErr = nil
			}
		case err := <-e:
			if err != nil {
				return err
			} else {
				numClosed++
				e = nil
			}
		case err := <-d:
			if err != nil {
				return err
			} else {
				numClosed++
				d = nil
			}
		}

		if numClosed == 4 {
			break
		}
	}

	// Acknowledge that we are finished
	var rFinished int
	e = g.EncodeAsync(finished)
	d = g.DecodeAsync(&rFinished)
	if err := <-e; err != nil {
		return err
	}
	if err := <-d; err != nil {
		return err
	}
	if rFinished != finished {
		return errors.New("Failed to acknowledge completion.")
	}

	log.Printf("Added %d txs to local mempool; sync done.", numAdded)
	return nil
}

// TODO: progress updates
func encodeTxs(txc <-chan []byte, g *GobConn) <-chan error {
	e := make(chan error)
	go func() {
		defer close(e)
		for tx := range txc {
			err := <-g.EncodeAsync(tx)
			if err != nil {
				e <- err
				return
			}
		}
	}()
	return e
}

// TODO: progress updates
func decodeTxs(txc chan<- []byte, n int, g *GobConn) <-chan error {
	e := make(chan error)
	go func() {
		defer close(e)
		defer close(txc)
		for i := 0; i < n; i++ {
			var tx []byte
			err := <-g.DecodeAsync(&tx)
			if err != nil {
				e <- err
				return
			}
			txc <- tx
		}
	}()
	return e
}

func getTxs(txc chan<- []byte, txList []string, cfg RPCConfig, done <-chan struct{}) <-chan error {
	e := make(chan error)
	go func() {
		defer close(e)
		defer close(txc)
		for _, txid := range txList {
			tx, err := getRawTransaction(txid, cfg)
			if err != nil {
				tx = nil
				select {
				case e <- err:
				case <-done:
					return
				}
			}
			select {
			case txc <- tx:
			case <-done:
				return
			}
		}
	}()
	return e
}

func sendTxs(txc <-chan []byte, cfg RPCConfig) <-chan error {
	e := make(chan error)
	go func() {
		defer close(e)
		for tx := range txc {
			if err := sendRawTransaction(tx, cfg); err != nil {
				e <- err
			}
		}
	}()
	return e
}

func setupClient(dialAddr string) (*GobConn, error) {
	conn, err := net.Dial("tcp", dialAddr)
	if err != nil {
		return nil, err
	}
	g := &GobConn{
		enc:  gob.NewEncoder(conn),
		dec:  gob.NewDecoder(conn),
		conn: conn,
	}
	return g, nil
}

func setupServer(listenAddr string) (*GobListener, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &GobListener{l: l}, nil
}

// getSendList returns the list of txs to send, specified by txid, in the order
// in which they should be sent. mempool is the local mempool; remoteHas tests
// if the remote node has a certain txid. remoteHas can be probabilistic
// (allowing bloom filters, for e.g.).
func getSendList(mempool map[string]MempoolEntry, remoteHas func(txid string) bool) []string {
	childMap := make(map[string][]string)
	for txid, entry := range mempool {
		for _, d := range entry.Depends {
			childMap[d] = append(childMap[d], txid)
		}
	}

	toSend := make(map[string]bool)
	numDepsRemoved := make(map[string]int)
	var sendList []string
	var stack []string

	// Initialize the stack with entries without mempool dependencies, and
	// mark the txs to send.
	for txid, entry := range mempool {
		if len(entry.Depends) == 0 {
			stack = append(stack, txid)
		}
		if !remoteHas(txid) {
			markToSend(txid, toSend, childMap)
		}
	}

	for len(stack) > 0 {
		// Pop
		newlen := len(stack) - 1
		txid := stack[newlen]
		stack = stack[:newlen]

		if toSend[txid] {
			sendList = append(sendList, txid)
		}

		for _, child := range childMap[txid] {
			n := numDepsRemoved[child] + 1
			if n == len(mempool[child].Depends) {
				// Child has dependencies satisfied, so add to the stack
				stack = append(stack, child)
			}
			if n > len(mempool[child].Depends) {
				panic("Num removed deps exceeded num deps.")
			}
			numDepsRemoved[child] = n
		}
	}

	return sendList
}

// markToSend marks the tx specified by txid as "to send", along with all its children
// (i.e. descendant txs).
func markToSend(txid string, toSend map[string]bool, childMap map[string][]string) {
	stack := []string{txid}
	for len(stack) > 0 {
		// Pop
		newlen := len(stack) - 1
		txid := stack[newlen]
		stack = stack[:newlen]

		if toSend[txid] {
			// Already marked
			continue
		}
		toSend[txid] = true
		for _, child := range childMap[txid] {
			stack = append(stack, child)
		}
	}
}
