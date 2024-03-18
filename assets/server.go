package assets

import (
	"context"

	"github.com/btcsuite/btcd/btcutil"
	clientrpc "github.com/lightninglabs/loop/looprpc"
)

type AssetsClientServer struct {
	manager *AssetsSwapManager

	clientrpc.UnimplementedAssetsClientServer
}

func NewAssetsServer(manager *AssetsSwapManager) *AssetsClientServer {
	return &AssetsClientServer{
		manager: manager,
	}
}

func (a *AssetsClientServer) SwapOut(ctx context.Context,
	req *clientrpc.SwapOutRequest) (*clientrpc.SwapOutResponse, error) {

	swap, err := a.manager.NewSwapOut(
		ctx, btcutil.Amount(req.Amt), req.Asset,
	)
	if err != nil {
		return nil, err
	}
	return &clientrpc.SwapOutResponse{
		SwapStatus: &clientrpc.AssetSwapStatus{
			SwapHash:   swap.SwapOut.SwapHash[:],
			SwapStatus: string(swap.SwapOut.State),
		},
	}, nil
}

func (a *AssetsClientServer) ListAssetSwaps(ctx context.Context,
	_ *clientrpc.ListAssetSwapsRequest) (*clientrpc.ListAssetSwapsResponse,
	error) {

	swaps, err := a.manager.ListSwapOutoutputs(ctx)
	if err != nil {
		return nil, err
	}

	rpcSwaps := make([]*clientrpc.AssetSwapStatus, 0, len(swaps))
	for _, swap := range swaps {
		rpcSwaps = append(rpcSwaps, &clientrpc.AssetSwapStatus{
			SwapHash:   swap.SwapHash[:],
			SwapStatus: string(swap.State),
		})
	}

	return &clientrpc.ListAssetSwapsResponse{
		SwapStatus: rpcSwaps,
	}, nil
}
