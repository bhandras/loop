package deposit

import (
	"context"
	"errors"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightningnetwork/lnd/keychain"
)

var (
	AssetDepositKeyFamily = int32(1122)

	// ErrManagerShuttingDown signals that the asset deposit manager is
	// shutting down and that no further calls should be made to it.
	ErrManagerShuttingDown = errors.New("asset deposit manager is " +
		"shutting down")
)

type State uint8

const (
	// StateInitiated indicates that the deposit has been initiated by the
	// client.
	StateInitiated State = 0

	// StateConfirmed indicates that the deposit has been confirmed on the
	// blockchain.
	StateConfirmed State = 1

	// StateExpired indicates that the deposit has expired.
	StateExpired State = 2

	// StateSpent indicates that the deposit has been spent.
	StateSpent State = 3

	// StateWithdrawn indicates that the deposit has been withdrawn.
	StateWithdrawn State = 4
)

func (s State) String() string {
	switch s {
	case StateInitiated:
		return "Initiated"

	case StateConfirmed:
		return "Confirmed"

	case StateSpent:
		return "Spent"

	case StateWithdrawn:
		return "Withdrawn"

	case StateExpired:
		return "Expired"

	default:
		return "Unknown"
	}
}

func (s State) Valid() bool {
	return s <= StateExpired
}

func (s State) IsFinal() bool {
	return s == StateSpent || s == StateWithdrawn
}

type Deposit struct {
	DepositID          string
	CreatedAt          time.Time
	AssetID            asset.ID
	Amount             uint64
	CSVExpiry          uint32
	ClientKeyDesc      keychain.KeyDescriptor
	ServerPubKey       btcec.PublicKey
	Addr               string
	State              State
	ConfirmationHeight uint32
	ProtocolVersion    AssetDepositProtocolVersion
}

func (d *Deposit) OnNewBlock(height uint32) {
	switch d.State {
	case StateInitiated:
		d.State = StateConfirmed
		d.ConfirmationHeight = height

	case StateConfirmed:
		if d.ConfirmationHeight+d.CSVExpiry <= height {
			d.State = StateExpired
		}
	}
}

type Manager struct {
	depositServiceClient swapserverrpc.AssetDepositServiceClient

	// walletKit is the backing lnd wallet to use for deposit operations.
	walletKit lndclient.WalletKitClient

	chainNotifier lndclient.ChainNotifierClient

	tapClient *assets.TapdClient

	addressParams *address.ChainParams

	store *SQLStore

	newDepositsChan chan *Deposit

	// currentHeight is the current block height as registered by the
	// deposit manager.
	currentHeight uint32

	activeDeposits map[string]*Deposit

	// callEnter is used to sequentialize calls to the batch handler's
	// main event loop.
	callEnter chan struct{}

	// callLeave is used to resume the execution flow of the batch handler's
	// main event loop.
	callLeave chan struct{}

	// quit is owned by the parent batcher and signals that the batch must
	// stop.
	quit chan struct{}
}

func NewDepositManager(
	depositServiceClient swapserverrpc.AssetDepositServiceClient,
	walletKit lndclient.WalletKitClient,
	chainNotifier lndclient.ChainNotifierClient,
	tapClient *assets.TapdClient, store *SQLStore) *Manager {

	return &Manager{
		depositServiceClient: depositServiceClient,
		walletKit:            walletKit,
		chainNotifier:        chainNotifier,
		tapClient:            tapClient,
		store:                store,
		newDepositsChan:      make(chan *Deposit),
		activeDeposits:       make(map[string]*Deposit),
		callEnter:            make(chan struct{}),
		callLeave:            make(chan struct{}),
		quit:                 make(chan struct{}),
	}
}

// scheduleNextCall schedules the next call to the manager's main event loop.
// It returns a function that must be called when the call is finished.
func (m *Manager) scheduleNextCall() (func(), error) {
	select {
	case m.callEnter <- struct{}{}:

	case <-m.quit:
		return func() {}, ErrManagerShuttingDown
	}

	return func() {
		m.callLeave <- struct{}{}
	}, nil
}

func (m *Manager) Run(ctx context.Context, currentHeight uint32) error {
	// First fetch all active deposits from the store.

	// Set the current height to the one passed in.
	m.currentHeight = currentHeight

	ctxc, cancel := context.WithCancel(ctx)
	defer func() {
		// Signal to the main event loop that it should stop.
		close(m.quit)

		cancel()
	}()

	blockChan, errChan, err := m.chainNotifier.RegisterBlockEpochNtfn(ctxc)
	if err != nil {
		log.Errorf("unable to register for block epoch "+
			"notifications: %v", err)

		return err
	}

	for {
		select {
		case <-m.callEnter:
			<-m.callLeave

		case blockHeight, ok := <-blockChan:
			if !ok {
				return nil
			}

			log.Debugf("Received block epoch notification: %v",
				blockHeight)

			m.currentHeight = uint32(blockHeight)
			// TODO(bhandras): check if any deposits have expired.

		case deposit := <-m.newDepositsChan:
			log.Debugf("Adding deposit to active deposits: %v",
				deposit.DepositID)

			m.activeDeposits[deposit.DepositID] = deposit

		case err := <-errChan:
			log.Errorf("received error from block epoch "+
				"notification: %v", err)

			return err

		case <-ctx.Done():
			return nil

		}
	}
}

// NewDeposit creates a new asset deposit with the given parameters.
func (m *Manager) NewDeposit(ctx context.Context, assetID asset.ID,
	amount uint64, csvExpiry uint32) (string, error) {

	clientKeyDesc, err := m.walletKit.DeriveNextKey(
		ctx, AssetDepositKeyFamily,
	)
	if err != nil {
		return "", err
	}

	resp, err := m.depositServiceClient.NewAssetDeposit(
		ctx, &swapserverrpc.NewAssetDepositServerReq{
			AssetId:   assetID[:],
			Amount:    amount,
			ClientKey: clientKeyDesc.PubKey.SerializeCompressed(),
			CsvExpiry: int32(csvExpiry),
		},
	)
	if err != nil {
		log.Errorf("Swap server was unable to create deposit: %v", err)

		return "", err
	}

	serverPubKey, err := btcec.ParsePubKey(resp.ServerKey)
	if err != nil {
		return "", err
	}

	deposit := &Deposit{
		CreatedAt:       time.Now(),
		DepositID:       resp.DepositId,
		AssetID:         assetID,
		Amount:          amount,
		CSVExpiry:       csvExpiry,
		ClientKeyDesc:   *clientKeyDesc,
		ServerPubKey:    *serverPubKey,
		Addr:            resp.DepositAddr,
		State:           StateInitiated,
		ProtocolVersion: CurrentProtocolVersion(),
	}

	err = m.store.AddAssetDeposit(ctx, deposit)
	if err != nil {
		log.Errorf("Unable to add deposit to store: %v", err)

		return "", err
	}

	m.newDepositsChan <- deposit

	return deposit.DepositID, nil
}

// GetBestBlock returns the current block height as registered by the manager.
func (m *Manager) GetBestBlock() (uint32, error) {
	done, err := m.scheduleNextCall()
	defer done()

	if err != nil {
		return 0, err
	}

	return m.currentHeight, nil
}

// ListDeposits returns all deposits that are in the given range of
// confirmations.
func (m *Manager) ListDeposits(ctx context.Context, minConfs, maxConfs uint32) (
	[]Deposit, error) {

	bestBlock, err := m.GetBestBlock()
	if err != nil {
		return nil, err
	}

	deposits, err := m.store.GetAllDeposits(ctx)
	if err != nil {
		return nil, err
	}

	filterConfs := minConfs != 0 || maxConfs != 0

	// Prefilter deposits based on the min/max confs.
	filteredDeposits := make([]Deposit, 0, len(deposits))
	for _, deposit := range deposits {
		// We don't want to include deposits that are not yet confirmed.
		if deposit.State == StateInitiated {
			continue
		}

		// Make sure that the best block is greater than the deposit's
		// confirmed height.
		if bestBlock < deposit.ConfirmationHeight {
			continue
		}

		if filterConfs {
			// Check that the deposit suits our min/max confs
			// criteria.
			confs := bestBlock - deposit.ConfirmationHeight
			if confs < minConfs || confs > maxConfs {
				continue
			}
		}

		filteredDeposits = append(filteredDeposits, deposit)
	}

	return filteredDeposits, nil
}
