package idx

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EthClient is the subset of go-ethereum's ethclient.Client used by
// Provider. It exists so tests (and alternative transports) can substitute
// a fake without dialing a real JSON-RPC endpoint. *ethclient.Client
// satisfies it.
type EthClient interface {
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockNumber(ctx context.Context) (uint64, error)
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error)
	Close()
}

// DialFunc establishes an EthClient connection to an RPC endpoint.
type DialFunc func(ctx context.Context, rawurl string) (EthClient, error)

func dialEthClient(ctx context.Context, rawurl string) (EthClient, error) {
	return ethclient.DialContext(ctx, rawurl)
}

var _ EthClient = (*ethclient.Client)(nil)
