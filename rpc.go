package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
)

type MempoolEntry struct {
	Depends []string
}

type RPCConfig struct {
	Addr, User, Password string
}

type request struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	Id     int64       `json:"id"`
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  interface{}     `json:"error"`
	Id     int64           `json:"id"`
}

func encodeRequest(method string, params interface{}) ([]byte, error) {
	r := request{
		Method: method,
		Params: params,
		Id:     rand.Int63(),
	}
	return json.Marshal(r)
}

func decodeResponse(r io.Reader, result interface{}) error {
	var resp response
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("%v", resp.Error)
	}
	if resp.Result == nil {
		return fmt.Errorf("Result is null")
	}
	return json.Unmarshal(resp.Result, result)
}

// getBlockCount makes a getblockcount call to a bitcoin JSON-RPC server.
func getBlockCount(cfg RPCConfig) (int, error) {
	r, err := encodeRequest("getblockcount", nil)
	if err != nil {
		return 0, err
	}
	respBody, err := getRespBody(r, cfg)
	if err != nil {
		return 0, err
	}
	defer respBody.Close()

	var height int
	err = decodeResponse(respBody, &height)
	return height, err
}

// getBlockHash makes a getblockhash call to a bitcoin JSON-RPC server.
func getBlockHash(height int, cfg RPCConfig) ([]byte, error) {
	r, err := encodeRequest("getblockhash", []int{height})
	respBody, err := getRespBody(r, cfg)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	var hashHex string
	if err := decodeResponse(respBody, &hashHex); err != nil {
		return nil, err
	}
	return hex.DecodeString(hashHex)
}

// getRawMempool makes a getrawmempool call to a bitcoin JSON-RPC server.
func getRawMempool(cfg RPCConfig) (map[string]MempoolEntry, error) {
	r, err := encodeRequest("getrawmempool", []bool{true})
	respBody, err := getRespBody(r, cfg)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	mempool := make(map[string]MempoolEntry)
	err = decodeResponse(respBody, &mempool)
	return mempool, err
}

// getRawTransaction makes a getrawtransaction call to a bitcoin JSON-RPC server
func getRawTransaction(txid string, cfg RPCConfig) ([]byte, error) {
	r, err := encodeRequest("getrawtransaction", []string{txid})
	respBody, err := getRespBody(r, cfg)
	if err != nil {
		return nil, err
	}
	defer respBody.Close()

	var txHex string
	if err := decodeResponse(respBody, &txHex); err != nil {
		return nil, err
	}
	return hex.DecodeString(txHex)
}

// getRawTransaction makes a sendrawtransaction call to a bitcoin JSON-RPC server
func sendRawTransaction(tx []byte, cfg RPCConfig) error {
	txHex := hex.EncodeToString(tx)
	r, err := encodeRequest("sendrawtransaction", []string{txHex})
	respBody, err := getRespBody(r, cfg)
	if err != nil {
		return err
	}
	defer respBody.Close()

	var v interface{}
	return decodeResponse(respBody, &v)
}

func getRespBody(reqbody []byte, cfg RPCConfig) (io.ReadCloser, error) {
	url := "http://" + cfg.Addr
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqbody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(cfg.User, cfg.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("Error %d: %s", resp.StatusCode, b)
	}
	return resp.Body, nil
}
