// Package idx_test holds this module's black-box test suite. Every file
// here lives in tests/ (not beside the package source) and exercises only
// idx's exported API, dot-imported for readability — Go requires white-box
// (unexported-access) tests to live in the package's own directory, so
// keeping tests physically separate means giving up direct access to
// unexported internals in favor of testing through the public surface,
// which is what a real consumer of this module would do anyway.
package idx_test

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// fakeEthClient is a minimal, concurrency-safe stand-in for *ethclient.Client
// used to unit test Provider/Pool without dialing a real RPC endpoint.
type fakeEthClient struct {
	mu sync.Mutex

	blockNumber      uint64
	blockNumberErr   error
	headers          map[uint64]*types.Header
	headerErr        error
	logs             []types.Log
	logsErr          error
	closed           int32
	filterLogsCalls  int32
	blockNumberCalls int32
	blockByNumberErr error
}

func newFakeEthClient() *fakeEthClient {
	return &fakeEthClient{headers: make(map[uint64]*types.Header)}
}

func (f *fakeEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blockByNumberErr != nil {
		return nil, f.blockByNumberErr
	}
	return nil, errNotImplemented
}

func (f *fakeEthClient) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return nil, errNotImplemented
}

func (f *fakeEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errNotImplemented
}

func (f *fakeEthClient) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return nil, errNotImplemented
}

func (f *fakeEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	atomic.AddInt32(&f.filterLogsCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logsErr != nil {
		return nil, f.logsErr
	}
	return f.logs, nil
}

func (f *fakeEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	return nil, errNotImplemented
}

func (f *fakeEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.headerErr != nil {
		return nil, f.headerErr
	}
	if h, ok := f.headers[number.Uint64()]; ok {
		return h, nil
	}
	return &types.Header{Number: number}, nil
}

func (f *fakeEthClient) BlockNumber(ctx context.Context) (uint64, error) {
	atomic.AddInt32(&f.blockNumberCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blockNumberErr != nil {
		return 0, f.blockNumberErr
	}
	return f.blockNumber, nil
}

func (f *fakeEthClient) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	return big.NewInt(0), nil
}

func (f *fakeEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, errNotImplemented
}

func (f *fakeEthClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}

func (f *fakeEthClient) Close() {
	atomic.AddInt32(&f.closed, 1)
}

func (f *fakeEthClient) setBlockNumber(n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockNumber = n
}

func (f *fakeEthClient) setLogs(logs []types.Log) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = logs
}

func (f *fakeEthClient) setHeader(number uint64, h *types.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headers[number] = h
}

func (f *fakeEthClient) setBlockNumberErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockNumberErr = err
}

func (f *fakeEthClient) setLogsErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logsErr = err
}

func (f *fakeEthClient) setHeaderErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headerErr = err
}

func (f *fakeEthClient) setBlockByNumberErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockByNumberErr = err
}

var errNotImplemented = errNotImplementedError{}

type errNotImplementedError struct{}

func (errNotImplementedError) Error() string { return "fakeEthClient: not implemented" }

var _ EthClient = (*fakeEthClient)(nil)
