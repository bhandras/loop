package loopd

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/aperture/l402"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop"
	"github.com/lightninglabs/loop/assets"
	"github.com/lightninglabs/loop/liquidity"
	"github.com/lightninglabs/loop/loopdb"
	"github.com/lightninglabs/loop/swap"
	"github.com/lightninglabs/loop/sweepbatcher"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/ticker"
)

// getClient returns an instance of the swap client.
func getClient(cfg *Config, swapDb loopdb.SwapStore,
	sweeperDb sweepbatcher.BatcherStore, lnd *lndclient.LndServices,
	assets *assets.TapdClient) (*loop.Client, func(), error) {

	// Default is not set for MaxLSATCost and MaxLSATFee to distinguish
	// it from user explicitly setting the option to default value.
	// So if MaxL402Cost and MaxLSATFee are not set in the config file
	// and command line, they are set to 0.
	const (
		defaultCost = l402.DefaultMaxCostSats
		defaultFee  = l402.DefaultMaxRoutingFeeSats
	)
	if cfg.MaxL402Cost != defaultCost && cfg.MaxLSATCost != 0 {
		return nil, nil, fmt.Errorf("both maxl402cost and maxlsatcost" +
			" were specified; they are not allowed together")
	}
	if cfg.MaxL402Fee != defaultFee && cfg.MaxLSATFee != 0 {
		return nil, nil, fmt.Errorf("both maxl402fee and maxlsatfee" +
			" were specified; they are not allowed together")
	}

	clientConfig := &loop.ClientConfig{
		ServerAddress:                        cfg.Server.Host,
		ProxyAddress:                         cfg.Server.Proxy,
		SwapServerNoTLS:                      cfg.Server.NoTLS,
		TLSPathServer:                        cfg.Server.TLSPath,
		Lnd:                                  lnd,
		AssetClient:                          assets,
		MaxL402Cost:                          btcutil.Amount(cfg.MaxL402Cost),
		MaxL402Fee:                           btcutil.Amount(cfg.MaxL402Fee),
		LoopOutMaxParts:                      cfg.LoopOutMaxParts,
		SkippedTxns:                          cfg.SkippedTxns,
		TotalPaymentTimeout:                  cfg.TotalPaymentTimeout,
		MaxPaymentRetries:                    cfg.MaxPaymentRetries,
		MaxStaticAddrHtlcFeePercentage:       cfg.MaxStaticAddrHtlcFeePercentage,
		MaxStaticAddrHtlcBackupFeePercentage: cfg.MaxStaticAddrHtlcBackupFeePercentage,
	}

	if cfg.MaxL402Cost == defaultCost && cfg.MaxLSATCost != 0 {
		warnf("Option maxlsatcost is deprecated and will be " +
			"removed. Switch to maxl402cost.")
		clientConfig.MaxL402Cost = btcutil.Amount(cfg.MaxLSATCost)
	}
	if cfg.MaxL402Fee == defaultFee && cfg.MaxLSATFee != 0 {
		warnf("Option maxlsatfee is deprecated and will be " +
			"removed. Switch to maxl402fee.")
		clientConfig.MaxL402Fee = btcutil.Amount(cfg.MaxLSATFee)
	}

	swapClient, cleanUp, err := loop.NewClient(
		cfg.DataDir, swapDb, sweeperDb, clientConfig,
	)
	if err != nil {
		return nil, nil, err
	}

	return swapClient, cleanUp, nil
}

func openDatabase(cfg *Config, chainParams *chaincfg.Params) (loopdb.SwapStore,
	*loopdb.BaseDB, error) { //nolint:unparam

	var (
		db     loopdb.SwapStore
		err    error
		baseDb loopdb.BaseDB
	)
	switch cfg.DatabaseBackend {
	case DatabaseBackendSqlite:
		infof("Opening sqlite3 database at: %v",
			cfg.Sqlite.DatabaseFileName)

		db, err = loopdb.NewSqliteStore(cfg.Sqlite, chainParams)
		if err != nil {
			return nil, nil, err
		}
		baseDb = *db.(*loopdb.SqliteSwapStore).BaseDB

	case DatabaseBackendPostgres:
		infof("Opening postgres database at: %v",
			cfg.Postgres.DSN(true))

		db, err = loopdb.NewPostgresStore(cfg.Postgres, chainParams)
		if err != nil {
			return nil, nil, err
		}
		baseDb = *db.(*loopdb.PostgresStore).BaseDB

	default:
		return nil, nil, fmt.Errorf("unknown database backend: %s",
			cfg.DatabaseBackend)
	}

	return db, &baseDb, nil
}

func getLiquidityManager(client *loop.Client) *liquidity.Manager {
	mngrCfg := &liquidity.Config{
		AutoloopTicker: ticker.NewForce(liquidity.DefaultAutoloopTicker),
		LoopOut:        client.LoopOut,
		LoopIn:         client.LoopIn,
		Restrictions: func(ctx context.Context, swapType swap.Type,
			initiator string) (*liquidity.Restrictions, error) {

			if swapType == swap.TypeOut {
				outTerms, err := client.Server.GetLoopOutTerms(ctx, initiator)
				if err != nil {
					return nil, err
				}

				return liquidity.NewRestrictions(
					outTerms.MinSwapAmount, outTerms.MaxSwapAmount,
				), nil
			}

			inTerms, err := client.Server.GetLoopInTerms(ctx, initiator)
			if err != nil {
				return nil, err
			}

			return liquidity.NewRestrictions(
				inTerms.MinSwapAmount, inTerms.MaxSwapAmount,
			), nil
		},
		Lnd:                  client.LndServices,
		Clock:                clock.NewDefaultClock(),
		LoopOutQuote:         client.LoopOutQuote,
		LoopInQuote:          client.LoopInQuote,
		ListLoopOut:          client.Store.FetchLoopOutSwaps,
		GetLoopOut:           client.Store.FetchLoopOutSwap,
		ListLoopIn:           client.Store.FetchLoopInSwaps,
		LoopInTerms:          client.LoopInTerms,
		LoopOutTerms:         client.LoopOutTerms,
		GetAssetPrice:        client.AssetClient.GetAssetPrice,
		MinimumConfirmations: minConfTarget,
		PutLiquidityParams:   client.Store.PutLiquidityParams,
		FetchLiquidityParams: client.Store.FetchLiquidityParams,
	}

	return liquidity.NewManager(mngrCfg)
}
