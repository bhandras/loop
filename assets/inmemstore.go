package assets

import (
	"context"
	"errors"
	"sync"

	"github.com/lightninglabs/loop/assets/swapin"
	"github.com/lightningnetwork/lnd/lntypes"
)

type InmemStore struct {
	swapIns map[lntypes.Hash]*swapin.SwapIn

	sync.Mutex
}

// NewInmemStore creates a new in-memory store for asset swaps.
func NewInmemStore() *InmemStore {
	return &InmemStore{
		swapIns: make(map[lntypes.Hash]*swapin.SwapIn),
	}
}

// CreateAssetSwapIn creates a new asset swap in in the store.
func (i *InmemStore) CreateAssetSwapIn(_ context.Context,
	swap *swapin.SwapIn) error {

	i.Lock()
	defer i.Unlock()

	i.swapIns[swap.SwapPreimage.Hash()] = swap
	return nil
}

// GetAssetSwapIn gets an asset swap in from the store.
func (i *InmemStore) GetAssetSwapIn(_ context.Context, swapHash []byte) (
	*swapin.SwapIn, error) {

	i.Lock()
	defer i.Unlock()

	swap, ok := i.swapIns[lntypes.Hash(swapHash)]
	if !ok {
		return nil, errors.New("swap not found")
	}

	return swap, nil
}

// UpdateAssetSwapIn updates an asset swap out in the store.
func (i *InmemStore) UpdateAssetSwapIn(ctx context.Context,
	swap *swapin.SwapIn) error {

	i.Lock()
	defer i.Unlock()

	i.swapIns[swap.SwapPreimage.Hash()] = swap
	return nil
}
