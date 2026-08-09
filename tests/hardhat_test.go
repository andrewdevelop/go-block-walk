//go:build hardhat

// This file only builds with `-tags hardhat` (see the Makefile's
// test-hardhat target), because — unlike every other test in this module —
// it talks to a real JSON-RPC node instead of a fake EthClient. It is
// intentionally excluded from `go test ./...` and CI's default run: it
// requires a Hardhat node already listening on HARDHAT_RPC_URL (defaults to
// http://127.0.0.1:8545), which this test does not start itself.
package idx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

func hardhatRPCURL() string {
	if url := os.Getenv("HARDHAT_RPC_URL"); url != "" {
		return url
	}
	return "http://127.0.0.1:8545"
}

// hardhatCall issues a raw JSON-RPC call against the Hardhat node and
// returns its "result" — used for methods (eth_accounts,
// eth_sendTransaction, eth_getTransactionReceipt, ...) that aren't part of
// the standard EthClient surface this package wraps.
func hardhatCall(t *testing.T, method string, params []any) json.RawMessage {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	if err != nil {
		t.Fatalf("hardhatCall(%s): marshal request: %v", method, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hardhatRPCURL(), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("hardhatCall(%s): build request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("hardhatCall(%s): request failed: %v", method, err)
	}
	defer resp.Body.Close()

	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("hardhatCall(%s): decode response: %v", method, err)
	}
	if result.Error != nil {
		t.Fatalf("hardhatCall(%s): node returned an error: %s", method, result.Error.Message)
	}
	return result.Result
}

// requireHardhat skips the test (rather than failing it) if no node is
// reachable at hardhatRPCURL() — this is an opt-in test, so a missing node
// should read as "you didn't set one up", not a real failure.
func requireHardhat(t *testing.T) ProviderPool {
	t.Helper()

	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{{
		Name:     "hardhat",
		URL:      hardhatRPCURL(),
		Priority: 1,
		Retry:    RetryConfig{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second},
	}}})
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := pool.BlockNumber(ctx); err != nil {
		t.Skipf("no Hardhat node reachable at %s (start one with `npx hardhat node` and re-run `make test-hardhat`): %v",
			hardhatRPCURL(), err)
	}

	return pool
}

// hardhatFirstAccount returns one of the node's default unlocked accounts,
// so transactions can be sent via eth_sendTransaction without signing.
func hardhatFirstAccount(t *testing.T) common.Address {
	t.Helper()

	var accounts []string
	if err := json.Unmarshal(hardhatCall(t, "eth_accounts", []any{}), &accounts); err != nil {
		t.Fatalf("eth_accounts: decode result: %v", err)
	}
	if len(accounts) == 0 {
		t.Fatal("eth_accounts returned no accounts — is this really a Hardhat node?")
	}
	return common.HexToAddress(accounts[0])
}

// hardhatLogEmitterInitCode is the smallest possible EVM bytecode that
// deterministically produces a real on-chain log without needing solc or an
// ABI: a contract-creation init code that just does LOG0(0, 0) — emit a
// log with no topics and no data — then RETURN(0, 0) — deploy empty
// runtime code. Disassembled:
//
//	PUSH1 0x00  PUSH1 0x00  LOG0    (offset=0, size=0)
//	PUSH1 0x00  PUSH1 0x00  RETURN  (offset=0, size=0)
const hardhatLogEmitterInitCode = "0x60006000a060006000f3"

// hardhatEmitLog deploys hardhatLogEmitterInitCode, which unconditionally
// emits exactly one log as part of the deployment transaction, and waits
// for its receipt. Hardhat auto-mines a block per transaction by default,
// so the log is on-chain by the time this returns.
func hardhatEmitLog(t *testing.T) (txHash common.Hash, contractAddress common.Address) {
	t.Helper()

	from := hardhatFirstAccount(t)
	var hash string
	if err := json.Unmarshal(hardhatCall(t, "eth_sendTransaction", []any{map[string]any{
		"from": from.Hex(),
		"data": hardhatLogEmitterInitCode,
		"gas":  "0x186a0",
	}}), &hash); err != nil {
		t.Fatalf("eth_sendTransaction: decode result: %v", err)
	}
	txHash = common.HexToHash(hash)

	var receipt struct {
		ContractAddress string `json:"contractAddress"`
		Logs            []struct {
			Address string `json:"address"`
		} `json:"logs"`
	}

	// The receipt should be available immediately (auto-mine), but poll
	// briefly in case this node is configured for interval mining instead.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw := hardhatCall(t, "eth_getTransactionReceipt", []any{hash})
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &receipt); err != nil {
				t.Fatalf("eth_getTransactionReceipt: decode result: %v", err)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if receipt.ContractAddress == "" {
		t.Fatalf("no receipt for tx %s after 10s — is this node auto-mining?", txHash)
	}
	if len(receipt.Logs) != 1 {
		t.Fatalf("expected exactly 1 log in the deployment receipt, got %d", len(receipt.Logs))
	}

	return txHash, common.HexToAddress(receipt.ContractAddress)
}

// hardhatLogListener records every log it receives, for later inspection —
// unlike the count-only countingListener used elsewhere, this test needs to
// check *which* log arrived.
type hardhatLogListener struct {
	mu   sync.Mutex
	logs []types.Log
}

func (l *hardhatLogListener) HandleLog(log types.Log, blockTimestamp uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, log)
	return nil
}

func (l *hardhatLogListener) snapshot() []types.Log {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]types.Log, len(l.logs))
	copy(out, l.logs)
	return out
}

// TestHardhat_PoolTalksToRealNode exercises Provider/Pool's RPC plumbing
// (dial, retry, rate limiting are all real here — only the network is a
// local devnet instead of mainnet) against an actual go-ethereum-compatible
// JSON-RPC server, rather than the fakeEthClient every other test uses.
func TestHardhat_PoolTalksToRealNode(t *testing.T) {
	pool := requireHardhat(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	head, err := pool.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}

	header, err := pool.HeaderByNumber(ctx, nil)
	if err != nil {
		t.Fatalf("HeaderByNumber(latest): %v", err)
	}
	if header.Number.Uint64() != head {
		t.Fatalf("expected the latest header's number (%d) to match BlockNumber (%d)", header.Number.Uint64(), head)
	}

	// Block 0 (genesis) must exist and be fetchable regardless of how far
	// the chain has since progressed.
	genesis, err := pool.HeaderByNumber(ctx, bigZero())
	if err != nil {
		t.Fatalf("HeaderByNumber(0): %v", err)
	}
	if genesis.Number.Uint64() != 0 {
		t.Fatalf("expected genesis header number 0, got %d", genesis.Number.Uint64())
	}

	if _, err := pool.LogsByBlockRange(ctx, 0, head); err != nil {
		t.Fatalf("LogsByBlockRange(0, %d): %v", head, err)
	}
}

// TestHardhat_IndexerTracksRealChain runs the full Indexer sync loop
// (Pool + Memory + EventDispatcher, all real) against the live node: it
// records the current head, deploys a contract whose init code emits a
// real log (hardhatEmitLog), and confirms not just that the indexer's last
// block advances but that the registered listener actually receives that
// exact log — proof the whole pipeline, including log dispatch, works
// end-to-end against a real chain, not just against fakes.
func TestHardhat_IndexerTracksRealChain(t *testing.T) {
	pool := requireHardhat(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	startBlock, err := pool.BlockNumber(ctx)
	cancel()
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}

	store := NewMemory()
	listener := &hardhatLogListener{}
	dispatcher := NewEventDispatcher([]BlockchainListener{listener})

	indexer, err := NewIndexer(IndexerConfig{
		Chain:         "hardhat-local",
		StartBlock:    startBlock + 1,
		BlockInterval: 200 * time.Millisecond,
	}, pool, store, dispatcher, WithLogger(silentLogger()))
	if err != nil {
		t.Fatalf("NewIndexer failed: %v", err)
	}

	if err := indexer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer indexer.Stop()

	txHash, contractAddress := hardhatEmitLog(t)

	waitFor(t, 15*time.Second, func() bool {
		last, _ := indexer.GetLastBlock()
		return last > startBlock
	})

	last, err := indexer.GetLastBlock()
	if err != nil {
		t.Fatalf("GetLastBlock: %v", err)
	}
	if last <= startBlock {
		t.Fatalf("expected the indexer to advance past block %d, got %d", startBlock, last)
	}

	// The chain head moving is not enough on its own — confirm the
	// listener actually received our transaction's log.
	logs := listener.snapshot()
	if len(logs) == 0 {
		t.Fatal("expected the listener to have received at least one log, got none")
	}

	var found bool
	for _, l := range logs {
		if l.TxHash != txHash {
			continue
		}
		found = true
		if l.Address != contractAddress {
			t.Fatalf("expected the log's address to be the deployed contract %s, got %s", contractAddress, l.Address)
		}
	}
	if !found {
		t.Fatalf("expected a log with TxHash %s among the %d log(s) the listener received", txHash, len(logs))
	}
}

func bigZero() *big.Int { return big.NewInt(0) }
