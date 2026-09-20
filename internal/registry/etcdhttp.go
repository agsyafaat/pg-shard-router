package registry

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// etcdClient talks to etcd via its built-in grpc-gateway JSON API
// (https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/) instead of the
// go.etcd.io/etcd/client/v3 module. It is deliberately dependency-free
// (net/http + encoding/json only): the clientv3 module pulls in grpc, zap,
// and golang.org/x/*, which is unnecessary weight for what this service
// needs (a handful of Range/Watch calls) and keeps the whole binary
// buildable with nothing beyond the Go standard library plus the Postgres
// driver.
//
// This talks to the *same* etcd cluster and key-space as a clientv3-based
// client would; it is not a different protocol, just a different client
// implementation of the same gRPC service via its HTTP transcoding.
type etcdClient struct {
	endpoints []string
	http      *http.Client
	username  string
	password  string

	token string // bearer token from /v3/auth/authenticate, if auth is enabled
}

func newEtcdClient(cfg EtcdConfig) *etcdClient {
	return &etcdClient{
		endpoints: cfg.Endpoints,
		http:      &http.Client{Timeout: 30 * time.Second},
		username:  cfg.Username,
		password:  cfg.Password,
	}
}

func (c *etcdClient) authenticate(ctx context.Context) error {
	if c.username == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]string{"name": c.username, "password": c.password})
	resp, err := c.postAny(ctx, "/v3/auth/authenticate", body)
	if err != nil {
		return fmt.Errorf("etcd auth: %w", err)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return fmt.Errorf("etcd auth: decode response: %w", err)
	}
	c.token = out.Token
	return nil
}

// kv is the base64-encoded key/value shape used throughout the gateway API.
type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type rangeResponse struct {
	Header struct {
		Revision string `json:"revision"`
	} `json:"header"`
	Kvs   []kv   `json:"kvs"`
	Count string `json:"count"`
}

// decodedKV is a Range/Watch result with the base64 layer already removed.
type decodedKV struct {
	Key   string
	Value []byte
}

// rangePrefix performs a linearizable Range request over every key with the
// given prefix, returning the decoded key/value pairs and the revision the
// read was taken at.
func (c *etcdClient) rangePrefix(ctx context.Context, prefix string) ([]decodedKV, int64, error) {
	reqBody, _ := json.Marshal(map[string]string{
		"key":       base64.StdEncoding.EncodeToString([]byte(prefix)),
		"range_end": base64.StdEncoding.EncodeToString([]byte(getPrefixRangeEnd(prefix))),
	})

	raw, err := c.postAny(ctx, "/v3/kv/range", reqBody)
	if err != nil {
		return nil, 0, err
	}

	var resp rangeResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, 0, fmt.Errorf("etcd range: decode response: %w", err)
	}

	out := make([]decodedKV, 0, len(resp.Kvs))
	for _, e := range resp.Kvs {
		key, err := base64.StdEncoding.DecodeString(e.Key)
		if err != nil {
			return nil, 0, fmt.Errorf("etcd range: decode key: %w", err)
		}
		val, err := base64.StdEncoding.DecodeString(e.Value)
		if err != nil {
			return nil, 0, fmt.Errorf("etcd range: decode value: %w", err)
		}
		out = append(out, decodedKV{Key: string(key), Value: val})
	}

	var rev int64
	fmt.Sscanf(resp.Header.Revision, "%d", &rev)
	return out, rev, nil
}

// watchEvent is a single decoded PUT/DELETE from a watch stream.
type watchEvent struct {
	Type     string // "PUT" or "DELETE"
	Key      string
	Value    []byte
	Revision int64
}

// watchBatch is one message from the streamed /v3/watch response.
type watchBatch struct {
	Revision int64
	Events   []watchEvent
	Canceled bool
	CancelReason string
}

// watchPrefix opens a long-lived streaming watch over every key with the
// given prefix, starting at startRevision, and delivers decoded batches to
// the returned channel until ctx is canceled or the stream ends. The
// channel is closed when the watch terminates for any reason; the caller
// (registry's watchLoop) is responsible for reconnecting.
func (c *etcdClient) watchPrefix(ctx context.Context, prefix string, startRevision int64) (<-chan watchBatch, error) {
	reqBody, _ := json.Marshal(map[string]any{
		"create_request": map[string]any{
			"key":            base64.StdEncoding.EncodeToString([]byte(prefix)),
			"range_end":      base64.StdEncoding.EncodeToString([]byte(getPrefixRangeEnd(prefix))),
			"start_revision": fmt.Sprintf("%d", startRevision),
		},
	})

	url, err := c.pickEndpoint()
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v3/watch", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		httpReq.Header.Set("Authorization", c.token)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("etcd watch: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("etcd watch: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	out := make(chan watchBatch)
	go func() {
		defer resp.Body.Close()
		defer close(out)

		dec := json.NewDecoder(resp.Body)
		for {
			var msg struct {
				Result struct {
					Header struct {
						Revision string `json:"revision"`
					} `json:"header"`
					Created  bool `json:"created"`
					Canceled bool `json:"canceled"`
					CancelReason string `json:"cancel_reason"`
					Events []struct {
						Type string `json:"type"`
						Kv   kv     `json:"kv"`
					} `json:"events"`
				} `json:"result"`
			}
			if err := dec.Decode(&msg); err != nil {
				return // stream ended or ctx canceled; caller reconnects
			}
			if msg.Result.Created {
				continue // initial ack, no events yet
			}

			var rev int64
			fmt.Sscanf(msg.Result.Header.Revision, "%d", &rev)

			batch := watchBatch{Revision: rev, Canceled: msg.Result.Canceled, CancelReason: msg.Result.CancelReason}
			for _, ev := range msg.Result.Events {
				key, _ := base64.StdEncoding.DecodeString(ev.Kv.Key)
				val, _ := base64.StdEncoding.DecodeString(ev.Kv.Value)
				typ := ev.Type
				if typ == "" {
					typ = "PUT" // gateway omits the field for PUT (enum zero value)
				}
				batch.Events = append(batch.Events, watchEvent{Type: typ, Key: string(key), Value: val, Revision: rev})
			}

			select {
			case out <- batch:
			case <-ctx.Done():
				return
			}
			if batch.Canceled {
				return
			}
		}
	}()

	return out, nil
}

// postAny POSTs body to path on the first reachable endpoint, retrying the
// next endpoint on connection failure. It re-authenticates once and retries
// on a 401, in case the token expired.
func (c *etcdClient) postAny(ctx context.Context, path string, body []byte) ([]byte, error) {
	var lastErr error
	for _, ep := range c.endpoints {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ep+path, bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if c.token != "" {
			httpReq.Header.Set("Authorization", c.token)
		}

		resp, err := c.http.Do(httpReq)
		if err != nil {
			lastErr = err
			continue
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized && c.username != "" && path != "/v3/auth/authenticate" {
			if authErr := c.authenticate(ctx); authErr == nil {
				continue // retry same endpoint with fresh token
			}
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("etcd %s: unexpected status %d: %s", path, resp.StatusCode, string(respBody))
			continue
		}
		return respBody, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("etcd %s: no endpoints configured", path)
	}
	return nil, lastErr
}

func (c *etcdClient) pickEndpoint() (string, error) {
	if len(c.endpoints) == 0 {
		return "", fmt.Errorf("etcd: no endpoints configured")
	}
	// A fuller implementation would round-robin/health-check; a single
	// preferred endpoint with the caller reconnecting on failure is enough
	// for a routing service backed by a small etcd cluster behind a VIP/LB,
	// which is the common deployment shape.
	return c.endpoints[0], nil
}

// getPrefixRangeEnd computes the etcd "range_end" that selects exactly the
// keys sharing the given prefix, matching clientv3's well-known algorithm:
// increment the last byte that isn't 0xff and truncate after it.
func getPrefixRangeEnd(prefix string) string {
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			out := make([]byte, i+1)
			copy(out, end[:i+1])
			out[i]++
			return string(out)
		}
	}
	return "\x00"
}
