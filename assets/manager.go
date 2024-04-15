package assets

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets/swapin"
	"github.com/lightninglabs/loop/fsm"
	loop_rpc "github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/loop/utils"
	"github.com/lightninglabs/taproot-assets/taprpc"
	"github.com/lightningnetwork/lnd/lntypes"
)

const (
	// DefaultSwapExpiry is the default expiry for a swap in blocks.
	DefaultSwapExpiry = 24

	FixedPrepayCost = 30000

	ClientKeyFamily = 696969
)

type Config struct {
	AssetClient *TapdClient
	Wallet      lndclient.WalletKitClient
	// ExchangeRateProvider is the exchange rate provider.
	ExchangeRateProvider *FixedExchangeRateProvider
	Signer               lndclient.SignerClient
	ChainNotifier        lndclient.ChainNotifierClient
	Router               lndclient.RouterClient
	LndClient            lndclient.LightningClient
	Invoices             lndclient.InvoicesClient
	Store                *PostgresStore
	SwapInStore          *InmemStore
	ServerClient         loop_rpc.AssetsSwapServerClient
	ChainParams          *chaincfg.Params
}

type AssetsSwapManager struct {
	cfg *Config

	expiryManager  *utils.ExpiryManager
	txConfManager  *utils.TxSubscribeConfirmationManager
	invoiceManager *utils.SubscribeInvoiceManager

	blockHeight    int32
	runCtx         context.Context
	activeSwapOuts map[lntypes.Hash]*OutFSM
	activeSwapIns  map[lntypes.Hash]*swapin.FSM

	sync.Mutex
}

func NewAssetSwapServer(config *Config) *AssetsSwapManager {
	return &AssetsSwapManager{
		cfg: config,

		activeSwapOuts: make(map[lntypes.Hash]*OutFSM),
		activeSwapIns:  make(map[lntypes.Hash]*swapin.FSM),
	}
}

func (m *AssetsSwapManager) Run(ctx context.Context, blockHeight int32) error {
	m.runCtx = ctx
	m.blockHeight = blockHeight

	// Get our tapd client info.
	tapdInfo, err := m.cfg.AssetClient.GetInfo(
		ctx, &taprpc.GetInfoRequest{},
	)
	if err != nil {
		return err
	}
	log.Infof("Tapd info: %v", tapdInfo)

	// Create our subscriptionManagers.
	m.expiryManager = utils.NewExpiryManager(m.cfg.ChainNotifier)
	m.txConfManager = utils.NewTxSubscribeConfirmationManager(
		m.cfg.ChainNotifier,
	)
	m.invoiceManager = utils.NewSubscribeInvoiceManager(m.cfg.Invoices)

	// Start the expiry manager.
	errChan := make(chan error, 1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.expiryManager.Start(ctx, blockHeight)
		if err != nil {
			log.Errorf("Expiry manager failed: %v", err)
			errChan <- err
			log.Errorf("Gude1")
		}
	}()

	// Recover all the active asset swap outs from the database.
	err = m.recoverSwapOuts(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-errChan:
			log.Errorf("Gude2")
			return err

		case <-ctx.Done():
			log.Errorf("Gude3")
			// wg.Wait()
			log.Errorf("Gude4")
			return nil
		}
	}
}

func (m *AssetsSwapManager) NewSwapOut(ctx context.Context,
	amt btcutil.Amount, asset []byte) (*OutFSM, error) {

	// Create a new out fsm.
	outFSM := NewOutFSM(m.runCtx, m.getFSMOutConfig())

	// Send the initial event to the fsm.
	err := outFSM.SendEvent(
		OnRequestAssetOut, &InitSwapOutContext{
			Amount:  amt,
			AssetId: asset,
		},
	)
	if err != nil {
		return nil, err
	}
	// Check if the fsm has an error.
	if outFSM.LastActionError != nil {
		return nil, outFSM.LastActionError
	}

	// Wait for the fsm to be in the state we expect.
	err = outFSM.DefaultObserver.WaitForState(
		ctx, time.Second*15, PayPrepay,
		fsm.WithAbortEarlyOnErrorOption(),
	)
	if err != nil {
		return nil, err
	}

	// Add the swap to the active swap outs.
	m.Lock()
	m.activeSwapOuts[outFSM.SwapOut.SwapHash] = outFSM
	m.Unlock()

	return outFSM, nil
}

// recoverSwapOuts recovers all the active asset swap outs from the database.
func (m *AssetsSwapManager) recoverSwapOuts(ctx context.Context) error {
	// Fetch all the active asset swap outs from the database.
	activeSwapOuts, err := m.cfg.Store.GetActiveAssetOuts(ctx)
	if err != nil {
		return err
	}

	for _, swapOut := range activeSwapOuts {
		log.Debugf("Recovering asset out %v with state %v",
			swapOut.SwapHash, swapOut.State)

		swapOutFSM := NewOutFSMFromSwap(
			ctx, m.getFSMOutConfig(), swapOut,
		)

		m.Lock()
		m.activeSwapOuts[swapOut.SwapHash] = swapOutFSM
		m.Unlock()

		// As SendEvent can block, we'll start a goroutine to process
		// the event.
		go func() {
			err := swapOutFSM.SendEvent(OnRecover, nil)
			if err != nil {
				log.Errorf("FSM %v Error sending recover "+
					"event %v, state: %v",
					swapOutFSM.SwapOut.SwapHash,
					err, swapOutFSM.SwapOut.State)
			}
		}()
	}

	return nil
}

// getFSMOutConfig returns a fsmconfig from the manager.
func (m *AssetsSwapManager) getFSMOutConfig() *FSMConfig {
	return &FSMConfig{
		TapdClient:            m.cfg.AssetClient,
		AssetClient:           m.cfg.ServerClient,
		BlockHeightSubscriber: m.expiryManager,
		TxConfSubscriber:      m.txConfManager,
		ExchangeRateProvider:  m.cfg.ExchangeRateProvider,
		Wallet:                m.cfg.Wallet,
		Router:                m.cfg.Router,

		Store:  m.cfg.Store,
		Signer: m.cfg.Signer,
	}
}

// getSwapInFSMConfig retruns a swap in FSM config.
func (m *AssetsSwapManager) getSwapInFSMConfig() *swapin.FSMConfig {
	return &swapin.FSMConfig{
		TapdClient:            m.cfg.AssetClient,
		AssetClient:           m.cfg.ServerClient,
		BlockHeightSubscriber: m.expiryManager,
		InvoiceSubscriber:     m.invoiceManager,
		TxConfSubscriber:      m.txConfManager,
		ExchangeRateProvider:  m.cfg.ExchangeRateProvider,
		Invoices:              m.cfg.Invoices,
		Wallet:                m.cfg.Wallet,
		Router:                m.cfg.Router,
		Signer:                m.cfg.Signer,
		Store:                 m.cfg.SwapInStore,
		ChainParams:           m.cfg.ChainParams,
	}
}

func (m *AssetsSwapManager) ListSwapOutoutputs(ctx context.Context) ([]*SwapOut,
	error) {

	return m.cfg.Store.GetAllAssetOuts(ctx)
}

func (m *AssetsSwapManager) NewSwapIn(ctx context.Context, assetID []byte,
	amt uint64) (*swapin.FSM, error) {

	// Create a new swap in FSM.
	swapInFSM := swapin.NewFSM(m.runCtx, m.getSwapInFSMConfig())

	// Send the initial event to the FSM.
	err := swapInFSM.SendEvent(
		swapin.EventOnRequestNew, &swapin.InitContext{
			AssetID: assetID,
			Amount:  amt,
		},
	)
	if err != nil {
		return nil, err
	}
	// Check if the fsm has an error.
	if swapInFSM.LastActionError != nil {
		return nil, swapInFSM.LastActionError
	}

	// Wait for the fsm to be in the state we expect.
	err = swapInFSM.DefaultObserver.WaitForState(
		ctx, time.Second*15, swapin.StateAcquiredQuote,
		fsm.WithAbortEarlyOnErrorOption(),
	)
	if err != nil {
		return nil, err
	}

	// Add the swap to the active swap ins.
	m.Lock()
	m.activeSwapIns[swapInFSM.SwapIn.SwapHash] = swapInFSM
	m.Unlock()

	return swapInFSM, nil
}

func (m *AssetsSwapManager) SwapInQuote(ctx context.Context,
	swapHash lntypes.Hash) (*swapin.Quote, error) {

	m.Lock()
	swapInFSM, ok := m.activeSwapIns[swapHash]
	m.Unlock()

	if !ok {
		return nil, fmt.Errorf("swapin not found")
	}

	// Wait for the fsm to be in the state we expect.
	err := swapInFSM.DefaultObserver.WaitForState(
		ctx, time.Second*5, swapin.StatePendingQuote,
		fsm.WithAbortEarlyOnErrorOption(),
	)
	if err != nil {
		return nil, err
	}

	err = swapInFSM.SendEvent(swapin.EventOnQuote, nil)
	if err != nil {
		return nil, err
	}

	if swapInFSM.LastActionError != nil {
		return nil, swapInFSM.LastActionError
	}

	if swapInFSM.SwapIn.State != swapin.StateAcquiredQuote {
		return nil, fmt.Errorf("unexpected state %v",
			swapInFSM.SwapIn.State)
	}

	return swapInFSM.SwapIn.LastQuote, nil
}

func (m *AssetsSwapManager) ExecuteSwapIn(ctx context.Context,
	swapHash lntypes.Hash) error {

	m.Lock()
	swapInFSM, ok := m.activeSwapIns[swapHash]
	m.Unlock()

	if !ok {
		return fmt.Errorf("swapin not found")
	}

	if swapInFSM.SwapIn.State != swapin.StateAcquiredQuote {
		return fmt.Errorf("unexpected state %v", swapInFSM.SwapIn.State)
	}

	err := swapInFSM.SendEvent(swapin.EventOnExecuteSwap, nil)
	if err != nil {
		return err
	}

	if swapInFSM.LastActionError != nil {
		return swapInFSM.LastActionError
	}

	if swapInFSM.SwapIn.State != swapin.StateSendSwapPayment {
		return fmt.Errorf("unexpected state %v",
			swapInFSM.SwapIn.State)
	}

	return nil
}
