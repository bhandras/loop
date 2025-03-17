package deposit

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr/musig2"
	"github.com/btcsuite/btcd/btcutil/psbt"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/wallet"
	"github.com/btcsuite/btcwallet/wtxmgr"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets"
	"github.com/lightninglabs/loop/assets/htlc"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightninglabs/taproot-assets/proof"
	"github.com/lightninglabs/taproot-assets/tappsbt"
	"github.com/lightninglabs/taproot-assets/taprpc"
	"github.com/lightninglabs/taproot-assets/taprpc/assetwalletrpc"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
)

var (
	AssetDepositKeyFamily = int32(1122)

	// ErrManagerShuttingDown signals that the asset deposit manager is
	// shutting down and that no further calls should be made to it.
	ErrManagerShuttingDown = errors.New("asset deposit manager is " +
		"shutting down")

	customLockExpiration = time.Hour * 24

	tmpServerSecNonce = [musig2.SecNonceSize]byte{
		// First 32 bytes: scalar k1
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0x01,

		// Second 32 bytes: scalar k2
		0x02, 0x13, 0x24, 0x35, 0x46, 0x57, 0x68, 0x79,
		0x8a, 0x9b, 0xac, 0xbd, 0xce, 0xdf, 0xf1, 0x02,
		0x14, 0x26, 0x38, 0x4a, 0x5c, 0x6e, 0x70, 0x82,
		0x94, 0xa6, 0xb8, 0xca, 0xdc, 0xee, 0xf1, 0x03,
	}
)

type State uint8

const (
	// StateInitiated indicates that the deposit has been initiated by the
	// client.
	StateInitiated State = 0

	// StatePending indicates that the deposit is pending confirmation on
	// the blockchain.
	StatePending State = 1

	// StateConfirmed indicates that the deposit has been confirmed on the
	// blockchain.
	StateConfirmed State = 2

	// StateExpired indicates that the deposit has expired.
	StateExpired State = 3

	// StateTimeoutSweepPublished indicates that the timeout sweep has been
	// published.
	StateTimeoutSweepPublished State = 4

	// StateSpent indicates that the deposit has been spent.
	StateSpent State = 5

	// StateWithdrawn indicates that the deposit has been withdrawn.
	StateWithdrawn State = 6
)

func (s State) String() string {
	switch s {
	case StateInitiated:
		return "Initiated"

	case StatePending:
		return "Pending"

	case StateConfirmed:
		return "Confirmed"

	case StateTimeoutSweepPublished:
		return "TimeoutSweepPublished"

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

type htlcPacket struct {
	swapKit       *htlc.SwapKit
	btcPacket     *psbt.Packet
	activeAssets  []*tappsbt.VPacket
	passiveAssets []*tappsbt.VPacket
	commitResp    *assetwalletrpc.CommitVirtualPsbtsResponse
}

// Deposit is the struct that holds all the information about an asset deposit.
type Deposit struct {
	*Kit

	// ID is the unique identifier for this deposit which will also be used
	// to store the deposit in both the server and client databases.
	ID string

	// CreatedAt is the time when the deposit was created (on the client).
	CreatedAt time.Time

	// Amount is the amount of asset to be deposited.
	// TODO(bhandras): potentially move to Kit.
	Amount uint64

	// Addr is the TAP deposit address where the asset will be sent.
	// TODO(bhandras): potentially move to Kit.
	Addr string

	// State is the deposit state.
	State State

	// Outpoint is the confirmed deposit outpoint.
	Outpoint wire.OutPoint

	// Proof is the proof of the deposit transfer.
	Proof *proof.Proof

	// AnchorRootHash is the root hash of the deposit anchor output.
	AnchorRootHash []byte

	// ConfirmationHeight is the block height at which the deposit was
	// confirmed.
	ConfirmationHeight uint32

	// PkScript is the pkScript of the deposit outpoint. This is used to
	// wait for spend and confirmation notifications.
	PkScript []byte

	htlcPacket *htlcPacket

	// ProtocolVerison is the protocol version of the deposit.
	ProtocolVersion AssetDepositProtocolVersion
}

type Manager struct {
	depositServiceClient swapserverrpc.AssetDepositServiceClient

	// walletKit is the backing lnd wallet to use for deposit operations.
	walletKit lndclient.WalletKitClient

	signer lndclient.SignerClient

	chainNotifier lndclient.ChainNotifierClient

	tapClient *assets.TapdClient

	addressParams address.ChainParams

	store *SQLStore

	// currentHeight is the current block height as registered by the
	// deposit manager.
	currentHeight uint32

	// pendingDeposits is a map of all pending (unconfirmed) deposits. The
	// key is the deposit ID.
	pendingDeposits map[string]*Deposit

	// activeDeposits is a map of all active (confirmed) deposits. The key
	// is the deposit ID.
	activeDeposits map[string]*Deposit

	// expiredDeposits is a map of all expired deposits. The key is the
	// deposit ID.
	expiredDeposits map[string]*Deposit

	// pendingSweeps is a map of all pending timeout sweeps. The key is the
	// deposit ID.
	pendingSweeps map[string]*Deposit

	// callEnter is used to sequentialize calls to the batch handler's
	// main event loop.
	callEnter chan struct{}

	// callLeave is used to resume the execution flow of the batch handler's
	// main event loop.
	callLeave chan struct{}

	// criticalErrChan is used to signal that a critical error has occurred
	// and that the manager should stop.
	criticalErrChan chan error

	// quit is owned by the parent batcher and signals that the batch must
	// stop.
	quit chan struct{}
}

// NewManager creates a new asset deposit manager.
func NewManager(
	depositServiceClient swapserverrpc.AssetDepositServiceClient,
	walletKit lndclient.WalletKitClient, signer lndclient.SignerClient,
	chainNotifier lndclient.ChainNotifierClient,
	tapClient *assets.TapdClient, store *SQLStore,
	params *chaincfg.Params) *Manager {

	return &Manager{
		depositServiceClient: depositServiceClient,
		walletKit:            walletKit,
		signer:               signer,
		chainNotifier:        chainNotifier,
		tapClient:            tapClient,
		store:                store,
		addressParams:        address.ParamsForChain(params.Name),
		pendingDeposits:      make(map[string]*Deposit),
		activeDeposits:       make(map[string]*Deposit),
		expiredDeposits:      make(map[string]*Deposit),
		pendingSweeps:        make(map[string]*Deposit),
		callEnter:            make(chan struct{}),
		callLeave:            make(chan struct{}),
		criticalErrChan:      make(chan error, 1),
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
	// Set the current height to the one passed in.
	m.currentHeight = currentHeight

	ctxc, cancel := context.WithCancel(ctx)
	defer func() {
		// Signal to the main event loop that it should stop.
		close(m.quit)

		cancel()
	}()

	err := m.recoverDeposits(ctx)
	if err != nil {
		log.Errorf("Unable to recover deposits: %v", err)

		return err
	}

	blockChan, blockErrChan, err := m.chainNotifier.RegisterBlockEpochNtfn(
		ctxc,
	)
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
			err := m.handleBlockEpoch(ctxc, uint32(blockHeight))
			if err != nil {
				return err
			}

		case err := <-blockErrChan:
			log.Errorf("received error from block epoch "+
				"notification: %v", err)

			return err

		case err := <-m.criticalErrChan:
			log.Errorf("stopping asset deposit manager due to "+
				"critical error: %v", err)

			return err

		case <-ctx.Done():
			return nil

		}
	}
}

func (m *Manager) recoverDeposits(ctx context.Context) error {
	// Fetch all active deposits from the store to kick-off the manager.
	activeDeposits, err := m.store.GetActiveDeposits(ctx)
	if err != nil {
		log.Errorf("Unable to fetch deposits from store: %v", err)

		return err
	}

	for i := range activeDeposits {
		d := &activeDeposits[i]
		log.Infof("Recovering deposit %v (state=%s)", d.ID, d.State)

		// Cache proof information for the deposit.
		// TODO(bhandras): should we also store the proof in the loop
		// store?
		err = m.cacheProofInfo(ctx, d)
		if err != nil {
			return err
		}

		// If the deposit has just been initiated, then we need to
		// ensure that it is funded.
		switch d.State {
		case StateInitiated:
			m.pendingDeposits[d.ID] = d
			err = m.fundDepositIfNeeded(ctx, d)
			if err != nil {
				log.Errorf("Unable to fund deposit %v: %v",
					d.ID, err)

				return err
			}

		case StateConfirmed:
			m.activeDeposits[d.ID] = d

		case StateExpired:
			m.expiredDeposits[d.ID] = d

		case StateTimeoutSweepPublished:
			m.pendingSweeps[d.ID] = d

		default:
			err := fmt.Errorf("Deposit %v in unknown state %s",
				d.ID, d.State)
			log.Errorf(err.Error())

			return err
		}
	}

	return nil
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

	kit, err := NewKit(
		clientKeyDesc.PubKey, serverPubKey, clientKeyDesc.KeyLocator,
		assetID, csvExpiry, &m.addressParams,
	)
	if err != nil {
		return "", err
	}

	deposit := &Deposit{
		Kit:             kit,
		ID:              resp.DepositId,
		CreatedAt:       time.Now(),
		Amount:          amount,
		Addr:            resp.DepositAddr,
		State:           StateInitiated,
		ProtocolVersion: CurrentProtocolVersion(),
	}

	err = m.store.AddAssetDeposit(ctx, deposit)
	if err != nil {
		log.Errorf("Unable to add deposit to store: %v", err)

		return "", err
	}

	err = m.addDeposit(ctx, deposit)
	if err != nil {
		log.Errorf("Unable to add deposit to active deposits: %v", err)

		return "", err
	}

	return deposit.ID, nil
}

// isDepositFunded checks if the deposit is funded with the expected amount. It
// does so by checking if there is a deposit output with the expected keys and
// amount in the list of transfers of the funder.
func (m *Manager) isDepositFunded(ctx context.Context, d *Deposit) (bool,
	*taprpc.AssetTransfer, int, error) {

	res, err := m.tapClient.ListTransfers(
		ctx, &taprpc.ListTransfersRequest{},
	)
	if err != nil {
		return false, nil, 0, err
	}

	transfer, outIndex, err := d.GetMatchingOut(d.Amount, res.Transfers)
	if err != nil {
		return false, nil, 0, err
	}

	if transfer == nil {
		return false, nil, 0, nil
	}

	return true, transfer, outIndex, nil
}

func (m *Manager) fundDepositIfNeeded(ctx context.Context, d *Deposit) error {
	// If the deposit is already funded then we don't need to fund it.
	if d.State != StateInitiated {
		return nil
	}

	// Now list transfers from tapd and check if the deposit is funded.
	funded, transfer, outIndex, err := m.isDepositFunded(ctx, d)
	if err != nil {
		log.Errorf("Unable to check if deposit %v is funded: %v", d.ID,
			err)

		return err
	}

	if !funded {
		// No funding transfer found, so we'll attempt to fund the
		// deposit by sending the asset to the deposit address.
		sendResp, err := m.tapClient.SendAsset(
			ctx, &taprpc.SendAssetRequest{
				TapAddrs: []string{d.Addr},
			},
		)
		if err != nil {
			log.Errorf("Unable to send asset to deposit %v: %v",
				d.ID, err)

			return err
		}

		// Find the funding outpoint in the transfer.
		transfer, outIndex, err = d.GetMatchingOut(
			d.Amount, []*taprpc.AssetTransfer{sendResp.Transfer},
		)
		if err != nil {
			log.Errorf("Unable to get funding out for %v: %v ",
				d.ID, err)

			return err
		}
	}

	log.Infof("Deposit %v is funded in anchor %x:%d", d.ID,
		transfer.AnchorTxHash, outIndex)

	// We can now update the deposit outpoint.
	outpoint, err := wire.NewOutPointFromString(
		transfer.Outputs[outIndex].Anchor.Outpoint,
	)
	if err != nil {
		log.Errorf("Unable to parse deposit outpoint %v: %v",
			transfer.Outputs[outIndex].Anchor.Outpoint, err)

		return err
	}

	d.Outpoint = *outpoint
	d.PkScript = transfer.Outputs[outIndex].Anchor.PkScript

	fmt.Printf("!!!! OP: %v PKSCRIPT: %x\n",
		d.Outpoint.String(), d.PkScript)

	/*
		pkScript, err := d.Kit.PkScriptFromTransferProof(
			transfer.Outputs[outIndex].NewProofBlob,
		)
		if err != nil {
			log.Errorf("Unable to get pkScript from transfer proof: %v",
				err)
			return err
		}
		fmt.Printf("!!!! PKSCRIPT: %x\n", pkScript)
	*/

	confirmed := transfer.AnchorTxBlockHeight != 0
	if !confirmed {
		// The deposit is not confirmed yet, so we need to wait until
		// the anchor transaction is confirmed on-chain.
		// TODO(bhandras): use the context of the manager here.
		err = m.waitForDepositConfirmation(context.TODO(), d)
		if err != nil {
			log.Errorf("Unable to wait for deposit confirmation: "+
				"%v", err)

			return err
		}
	} else {
		m.markDepositConfirmed(ctx, d, transfer.AnchorTxBlockHeight)
	}

	return nil
}

func (m *Manager) criticalError(err error) {
	select {
	case m.criticalErrChan <- err:
	default:
	}
}

func (m *Manager) waitForDepositConfirmation(ctx context.Context,
	d *Deposit) error {

	// TODO: we should use a configurable number of confirmations.
	const numConfs = 3
	confChan, errChan, err := m.chainNotifier.RegisterConfirmationsNtfn(
		ctx, nil, d.PkScript, numConfs, int32(m.currentHeight),
	)
	if err != nil {
		return err
	}

	go func() {
		log.Infof("Waiting for deposit %v confirmation", d.ID)

		select {
		case conf := <-confChan:
			done, err := m.scheduleNextCall()
			defer done()
			if err != nil {
				log.Errorf("Unable to schedule next call: %v",
					err)

				return
			}

			err = m.markDepositConfirmed(ctx, d, conf.BlockHeight)
			if err != nil {
				log.Errorf("Unable to mark deposit %v as "+
					"confirmed: %v", d.ID, err)

				return
			}

		case err := <-errChan:
			fmt.Printf("Received error from confirmation "+
				"notification: %v", err)
			m.criticalError(err)
		}
	}()

	return nil
}

func (m *Manager) cacheProofInfo(ctx context.Context, d *Deposit) error {
	proofFile, err := d.ExportProof(ctx, m.tapClient, &d.Outpoint)
	if err != nil {
		log.Errorf("Unable to export proof for deposit %v: %v", d.ID,
			err)

		return err
	}

	// Import the proof in order to be able to spend the deposit later on
	// either into an HTLC or a timeout sweep.
	// TODO(bhandras): check/handle if/when the proof is already imported.
	depositProof, err := m.tapClient.ImportProofFile(
		ctx, proofFile.RawProofFile,
	)
	if err != nil {
		return err
	}
	d.Proof = depositProof

	// Verify that the proof is valid for the deposit and get the root hash
	// which we may use later when signing the HTLC transaction.
	anchorRootHash, err := d.VerifyProof(depositProof)
	if err != nil {
		log.Errorf("failed to verify deposity proof: %v", err)

		return err
	}
	d.AnchorRootHash = anchorRootHash

	return nil
}

// markDepositConfirmed marks the deposit as confirmed in the store and moves it
// to the active deposits map. It also updates the confirmation height of the
// deposit.
func (m *Manager) markDepositConfirmed(ctx context.Context, d *Deposit,
	blockHeight uint32) error {

	d.ConfirmationHeight = blockHeight
	d.State = StateConfirmed

	err := m.store.MarkDepositConfirmed(
		ctx, d.ID, d.Outpoint.String(), d.PkScript, blockHeight,
	)
	if err != nil {
		return err
	}

	// Move the deposit to the active deposits map.
	delete(m.pendingDeposits, d.ID)
	m.activeDeposits[d.ID] = d

	err = m.cacheProofInfo(ctx, d)
	if err != nil {
		return err
	}

	log.Infof("Deposit %v is confirmed at block %v", d.ID, blockHeight)

	return nil
}

func (m *Manager) addDeposit(ctx context.Context, deposit *Deposit) error {
	done, err := m.scheduleNextCall()
	defer done()

	if err != nil {
		return err
	}

	m.pendingDeposits[deposit.ID] = deposit

	return m.fundDepositIfNeeded(ctx, deposit)
}

func (m *Manager) handleBlockEpoch(ctx context.Context, height uint32) error {
	for _, d := range m.activeDeposits {
		log.Infof("Checking if deposit %v is expired, expiry=%v", d.ID,
			d.ConfirmationHeight+d.CsvExpiry)

		if height < d.ConfirmationHeight+d.CsvExpiry {
			continue
		}

		d.State = StateExpired
		err := m.store.UpdateDepositState(ctx, d.ID, d.State)
		if err != nil {
			log.Errorf("Unable to update deposit %v state: %v",
				d.ID, err)

			return err
		}

		m.expiredDeposits[d.ID] = d
		delete(m.activeDeposits, d.ID)
	}

	// Attempt to republish any pending sweeps that are not yet confirmed.
	for _, d := range m.pendingSweeps {
		log.Infof("Republishing timeout sweep for deposit %v", d.ID)
		err := m.publishTimeoutSweep(ctx, d)
		if err != nil {
			log.Infof("Unable to republish timeout sweep for "+
				"deposit %v: %v", d.ID, err)
		}
	}

	// Now publish the timeout sweeps for all expired deposits and also
	// move them to the pending sweeps map.
	for _, d := range m.expiredDeposits {
		log.Infof("Publishing timeout sweep for deposit %v", d.ID)

		// At this point we can also start monitoring the sweep by
		// waiting for the deposit UTXO to be spent.
		err := m.waitForDepositSpend(ctx, d)
		if err != nil {
			log.Errorf("Unable to wait for deposit %v "+
				"spend: %v", d.ID, err)

			return err
		}

		err = m.publishTimeoutSweep(ctx, d)
		if err != nil {
			log.Errorf("Unable to publish timeout sweep for "+
				"deposit %v: %v", d.ID, err)
		}

		d.State = StateTimeoutSweepPublished
		err = m.store.UpdateDepositState(ctx, d.ID, d.State)
		if err != nil {
			log.Errorf("Unable to update deposit %v state: %v",
				d.ID, err)

			return err
		}

		delete(m.expiredDeposits, d.ID)
		m.pendingSweeps[d.ID] = d
	}

	return nil
}

// depositLockID converts a deposit ID to a lock ID. The lock ID is used to
// lock inputs used for the deposit sweep transaction. Note that we assume that
// the deposit ID is a hex-encoded string of the same length as the lock ID.
func depositLockID(depositID string) (wtxmgr.LockID, error) {
	var lockID wtxmgr.LockID
	depositIDBytes, err := hex.DecodeString(depositID)
	if err != nil {
		return wtxmgr.LockID{}, err
	}

	if len(depositIDBytes) != len(lockID) {
		return wtxmgr.LockID{}, fmt.Errorf("invalid deposit ID "+
			"length: %d", len(depositIDBytes))
	}

	copy(lockID[:], depositIDBytes)

	return lockID, nil
}

func (m *Manager) publishTimeoutSweep(ctx context.Context, d *Deposit) error {
	// Fetch the (full) proof of the deposit from tapd.
	rpcProofFile, err := d.ExportProof(ctx, m.tapClient, &d.Outpoint)
	if err != nil {
		log.Errorf("Unable to export proof for deposit %v: %v", d.ID,
			err)

		return err
	}

	// We only need the last proof which is the deposit transfer proof.
	depositProof, err := m.tapClient.ImportProofFile(
		ctx, rpcProofFile.RawProofFile,
	)
	if err != nil {
		log.Errorf("Unable to get last proof from file %v: %v", d.ID,
			err)

		return err
	}

	// Generate a new address for the timeout sweep.
	rpcTimeoutSweepAddr, err := m.tapClient.NewAddr(
		ctx, &taprpc.NewAddrRequest{
			AssetId: d.AssetID[:],
			Amt:     d.Amount,
		},
	)
	if err != nil {
		log.Errorf("Unable to create timeout sweep address: %v", err)

		return err
	}

	log.Infof("Timeout sweep addres for deposit %v: %v", d.ID,
		rpcTimeoutSweepAddr.Encoded)

	timeoutSweepAddr, err := address.DecodeAddress(
		rpcTimeoutSweepAddr.Encoded, &m.addressParams,
	)
	if err != nil {
		log.Errorf("Unable to decode timeout sweep address: %v", err)

		return err
	}

	// Now we can create the sweep vpacket which is simply sweeping the
	// asset on the OP_TRUE output to the timeout sweep address.
	sweepVpkt, err := assets.CreateOpTrueSweepVpkt(
		ctx, []*proof.Proof{depositProof}, timeoutSweepAddr,
		&m.addressParams,
	)
	if err != nil {
		log.Errorf("Unable to create timeout sweep vpkt: %v", err)

		return err
	}

	// TODO(bhandras): use a conf target from the user.
	feeRate, err := m.walletKit.EstimateFeeRate(ctx, 2)
	if err != nil {
		log.Errorf("Unable to estimate fee rate: %v", err)

		return err
	}

	// Gather the list of leased UTXOs that are used for the deposit sweep.
	// This is needed to ensure that the UTXOs are correctly reused if we
	// re-publish the deposit sweep.
	leases, err := m.walletKit.ListLeases(ctx)
	if err != nil {
		log.Errorf("Unable to list leases: %v", err)

		return err
	}

	customLockID, err := depositLockID(d.ID)
	if err != nil {
		return err
	}

	var leasedUtxos []lndclient.LeaseDescriptor
	for _, lease := range leases {
		if lease.LockID == customLockID {
			leasedUtxos = append(leasedUtxos, lease)
		}
	}

	// By committing the virtual transaction to the BTC template we
	// created, Alice's lnd node will fund the BTC level transaction with
	// an input to pay for the fees (and it will also add a change output).
	timeoutSweepBtcPkt, activeAssets, passiveAssets, commitResp, err :=
		m.tapClient.PrepareAndCommitVirtualPsbts(
			ctx, sweepVpkt, feeRate.FeePerVByte(), nil,
			m.addressParams.Params, leasedUtxos,
			customLockID, customLockExpiration,
		)
	if err != nil {
		log.Errorf("Unable to prepare and commit virtual psbt: %v",
			err)
	}

	for _, utxo := range commitResp.LndLockedUtxos {
		fmt.Printf("!!!! LOCKED UTXO: %v\n", spew.Sdump(utxo))
	}

	// Create the witness for the timeout sweep.
	witness, err := d.CreateTimeoutWitness(
		ctx, m.signer, depositProof, timeoutSweepBtcPkt,
		d.KeyLocator,
	)
	if err != nil {
		log.Errorf("Unable to create timeout witness: %v", err)

		return err
	}

	// Now add the witness to the sweep packet.
	var buf bytes.Buffer
	err = psbt.WriteTxWitness(&buf, witness)
	if err != nil {
		log.Errorf("Unable to write witness to buffer: %v", err)

		return err
	}

	timeoutSweepBtcPkt.Inputs[0].SighashType = txscript.SigHashDefault
	timeoutSweepBtcPkt.Inputs[0].FinalScriptWitness = buf.Bytes()

	// Sign and finalize the sweep packet.
	signedBtcPacket, err := m.walletKit.SignPsbt(
		ctx, timeoutSweepBtcPkt,
	)
	if err != nil {
		log.Errorf("Unable to sign timeout sweep packet: %v", err)

		return err
	}

	finalizedBtcPacket, _, err := m.walletKit.FinalizePsbt(
		ctx, signedBtcPacket, "",
	)
	if err != nil {
		log.Errorf("Unable to finalize timeout sweep packet: %v",
			err)

		return err
	}

	log.Infof("Registering deposit transfer for deposit %v", d.ID)

	// Register the deposit transfer. This essentially materializes an
	// asset "out of thin air" to ensure that LogAndPublish succeeds and
	// the asset balance will be updated correctly.
	depositScriptKey := depositProof.Asset.ScriptKey
	_, err = m.tapClient.RegisterTransfer(
		ctx, &taprpc.RegisterTransferRequest{
			AssetId:   d.AssetID[:],
			GroupKey:  nil,
			ScriptKey: depositScriptKey.PubKey.SerializeCompressed(),
			Outpoint: &taprpc.OutPoint{
				Txid:        d.Outpoint.Hash[:],
				OutputIndex: d.Outpoint.Index,
			},
		},
	)

	if err != nil {
		if !strings.Contains(err.Error(), "proof already exists") {
			log.Errorf("Unable to register deposit transfer: %v",
				err)

			return err
		}
	}

	log.Infof("Publishing timeout sweep for deposit %v", d.ID)
	// Publish the timeout sweep and log the transfer.
	sendAssetResp, err := m.tapClient.LogAndPublish(
		ctx, finalizedBtcPacket, activeAssets, passiveAssets,
		commitResp,
	)
	if err != nil {
		log.Errorf("Unable to publish timeout sweep: %v", err)

		return err
	}
	log.Infof("Timeout sweep for deposit %v published: %x", d.ID,
		sendAssetResp.Transfer.AnchorTxHash)

	return nil
}

func (m *Manager) releaseDepositSweepInputs(ctx context.Context,
	d *Deposit) error {

	lockID, err := depositLockID(d.ID)
	if err != nil {
		return err
	}

	leases, err := m.walletKit.ListLeases(ctx)
	if err != nil {
		return err
	}

	for _, lease := range leases {
		if lease.LockID != lockID {
			continue
		}

		// Unlock any UTXOs that were used for the deposit sweep.
		err = m.walletKit.ReleaseOutput(ctx, lockID, lease.Outpoint)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) handleDepositSpend(ctx context.Context, d *Deposit,
	spendTxHash chainhash.Hash) error {

	switch d.State {
	case StateTimeoutSweepPublished:
		log.Infof("Deposit %s withdrawn in: %s", d.ID, spendTxHash)
		d.State = StateWithdrawn

		err := m.releaseDepositSweepInputs(ctx, d)
		if err != nil {
			log.Errorf("Unable to release deposit sweep inputs: "+
				"%v", err)

			return err
		}

	default:
		err := fmt.Errorf("Spent deposit %s in unexpected state %s",
			d.ID, d.State)
		log.Errorf(err.Error())

		return err
	}

	// TODO(bhandras): save the spend details to the store.
	err := m.store.UpdateDepositState(ctx, d.ID, d.State)
	if err != nil {
		log.Errorf("Unable to update deposit %v state: %v", d.ID, err)

		return err
	}

	// Sanity check that the deposit is in the pending sweeps map.
	if _, ok := m.pendingSweeps[d.ID]; !ok {
		log.Errorf("Deposit %v not found in pending deposits", d.ID)
	}

	delete(m.pendingSweeps, d.ID)

	return nil
}

func (m *Manager) waitForDepositSpend(ctx context.Context, d *Deposit) error {
	// TOOD(bhandras): we should use the pkScript from the deposit.
	spendChan, errChan, err := m.chainNotifier.RegisterSpendNtfn(
		ctx, &d.Outpoint, d.PkScript, int32(m.currentHeight),
	)
	if err != nil {
		return err
	}

	go func() {
		log.Infof("Waiting for deposit %v spend", d.ID)

		select {
		case spend := <-spendChan:
			done, err := m.scheduleNextCall()
			defer done()
			if err != nil {
				log.Errorf("Unable to schedule next call: %v",
					err)

				return
			}

			err = m.handleDepositSpend(ctx, d, *spend.SpenderTxHash)
			if err != nil {
				m.criticalError(err)
			}

		case err := <-errChan:
			m.criticalError(err)
		}
	}()

	return nil
}

// TODO(bhandras): add support for using multiple deposits.
func (m *Manager) getHTLC(ctx context.Context, d *Deposit,
	swapHash lntypes.Hash, htlcExpiry uint32) (*psbt.Packet, error) {

	// Genearate the HTLC address that will be used to sweep the deposit to
	// in case the client is uncooperative.
	rpcHtlcAddr, swapKit, err := d.NewHtlcAddr(
		ctx, m.tapClient, d.Amount, swapHash, htlcExpiry,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create htlc addr: %v", err)
	}

	htlcAddr, err := address.DecodeAddress(
		rpcHtlcAddr.Encoded, &m.addressParams,
	)
	if err != nil {
		return nil, err
	}

	// Now we can create the sweep vpacket that'd sweep the deposited
	// assets to the HTLC output.
	depositSpendVpkt, err := assets.CreateOpTrueSweepVpkt(
		ctx, []*proof.Proof{d.Proof}, htlcAddr, &m.addressParams,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create deposit "+
			"spend vpacket: %v", err)
	}

	// By committing the virtual transaction to the BTC template we
	// created, our lnd node will fund the BTC level transaction with an
	// input to pay for the fees. We'll further add a change output to the
	// transaction that will be generated using the above key descriptor.
	//
	// TODO(bhandras): use custom lock id and expiration.
	feeRate := chainfee.SatPerVByte(0)
	customLockID, err := depositLockID(d.ID)
	if err != nil {
		return nil, err
	}

	depositSpendBtcPkt, activeAssets, passiveAssets, commitResp, err :=
		m.tapClient.PrepareAndCommitVirtualPsbts(
			ctx, depositSpendVpkt, feeRate, nil,
			m.addressParams.Params, nil, customLockID,
			customLockExpiration,
		)
	if err != nil {
		return nil, fmt.Errorf("Deposit spend HTLC prepare and "+
			"commit failed, deposit_id=%v: %v", d.ID, err)
	}

	log.Infof("Deposit spend BTC packet: %v",
		spew.Sdump(depositSpendBtcPkt))

	// TODO:(bhandras): store the resulting btc packet as well as active
	// and passive assets. Also ideally we want a zero fee rate packet with
	// no locked inputs. Likely solution is to change PublishAndLog to allow
	// submitting v3 packages More here:
	// keybase://chat/lightninglabs#taproot-assets/48138.
	d.htlcPacket = &htlcPacket{
		swapKit:       swapKit,
		btcPacket:     depositSpendBtcPkt,
		activeAssets:  activeAssets,
		passiveAssets: passiveAssets,
		commitResp:    commitResp,
	}

	return depositSpendBtcPkt, nil
}

// secNonceToPubNonce takes our two secrete nonces, and produces their two
// corresponding EC points, serialized in compressed format.
func secNonceToPubNonce(
	secNonce [musig2.SecNonceSize]byte) [musig2.PubNonceSize]byte {

	var k1Mod, k2Mod btcec.ModNScalar
	k1Mod.SetByteSlice(secNonce[:btcec.PrivKeyBytesLen])
	k2Mod.SetByteSlice(secNonce[btcec.PrivKeyBytesLen:])

	var r1, r2 btcec.JacobianPoint
	btcec.ScalarBaseMultNonConst(&k1Mod, &r1)
	btcec.ScalarBaseMultNonConst(&k2Mod, &r2)

	// Next, we'll convert the key in jacobian format to a normal public
	// key expressed in affine coordinates.
	r1.ToAffine()
	r2.ToAffine()
	r1Pub := btcec.NewPublicKey(&r1.X, &r1.Y)
	r2Pub := btcec.NewPublicKey(&r2.X, &r2.Y)

	var pubNonce [musig2.PubNonceSize]byte

	// The public nonces are serialized as: R1 || R2, where both keys are
	// serialized in compressed format.
	copy(pubNonce[:], r1Pub.SerializeCompressed())
	copy(
		pubNonce[btcec.PubKeyBytesLenCompressed:],
		r2Pub.SerializeCompressed(),
	)

	return pubNonce
}

func (m *Manager) partialSignMuSig2(ctx context.Context, d *Deposit,
	cosignerNonce [musig2.PubNonceSize]byte, message [32]byte) (
	[musig2.PubNonceSize]byte, []byte, error) {

	signers := [][]byte{
		d.FunderKey.SerializeCompressed(),
		d.CoSignerKey.SerializeCompressed(),
	}

	session, err := m.signer.MuSig2CreateSession(
		ctx, input.MuSig2Version100RC2,
		&keychain.KeyLocator{
			Family: d.KeyLocator.Family,
			Index:  d.KeyLocator.Index,
		}, signers, lndclient.MuSig2TaprootTweakOpt(
			d.AnchorRootHash, false,
		),
	)
	if err != nil {
		return [musig2.PubNonceSize]byte{}, nil, err
	}

	fmt.Printf("!!! root hash: %x\n", d.AnchorRootHash)

	_, err = m.signer.MuSig2RegisterNonces(
		ctx, session.SessionID,
		[][musig2.PubNonceSize]byte{cosignerNonce},
	)
	if err != nil {
		return [musig2.PubNonceSize]byte{}, nil, err
	}

	clientPartialSig, err := m.signer.MuSig2Sign(
		ctx, session.SessionID, message, true,
	)
	if err != nil {
		return [musig2.PubNonceSize]byte{}, nil, err
	}

	fmt.Printf("!!! client partial sig: %x\n", clientPartialSig)
	fmt.Printf("!!! client nonce: %x\n", session.PublicNonce)

	return session.PublicNonce, clientPartialSig, nil
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

	// Only filter based on confirmations if the user has set a min or max
	// confs.
	filterConfs := minConfs != 0 || maxConfs != 0

	// Prefilter deposits based on the min/max confs.
	filteredDeposits := make([]Deposit, 0, len(deposits))
	for _, deposit := range deposits {
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

// getSigHash calculates the signature hash for the given transaction.
func getSigHash(tx *wire.MsgTx, idx int,
	prevOutFetcher txscript.PrevOutputFetcher) ([32]byte, error) {

	var sigHash [32]byte

	sigHashes := txscript.NewTxSigHashes(tx, prevOutFetcher)
	taprootSigHash, err := txscript.CalcTaprootSignatureHash(
		sigHashes, txscript.SigHashDefault, tx, idx, prevOutFetcher,
	)
	if err != nil {
		return sigHash, err
	}

	copy(sigHash[:], taprootSigHash)

	return sigHash, nil
}

func (m *Manager) CoSignHTLC(ctx context.Context, depositID string) error {
	done, err := m.scheduleNextCall()
	defer done()

	if err != nil {
		return err
	}

	serverNonce := secNonceToPubNonce(tmpServerSecNonce)

	deposit, ok := m.activeDeposits[depositID]
	if !ok {
		return fmt.Errorf("deposit %v not available", depositID)
	}

	preimage := lntypes.Preimage{1, 2, 3}
	swapHash := preimage.Hash()
	htlcExpiry := uint32(100)

	htlcPkt, err := m.getHTLC(ctx, deposit, swapHash, htlcExpiry)
	if err != nil {
		log.Errorf("Unable to get HTLC packet: %v", err)

		return err
	}

	prevOutFetcher := wallet.PsbtPrevOutputFetcher(htlcPkt)
	sigHash, err := getSigHash(htlcPkt.UnsignedTx, 0, prevOutFetcher)
	if err != nil {
		return err
	}

	nonce, partialSig, err := m.partialSignMuSig2(
		ctx, deposit, serverNonce, sigHash,
	)
	if err != nil {
		log.Errorf("Unable to partial sign HTLC: %v", err)

		return err
	}

	var pktBuf bytes.Buffer
	err = htlcPkt.Serialize(&pktBuf)
	if err != nil {
		log.Errorf("Unable to write HTLC packet: %v", err)
		return err
	}

	_, err = m.depositServiceClient.CoSignHtlc(ctx,
		&swapserverrpc.CoSignHtlcReq{
			DepositId:  depositID,
			Psbt:       pktBuf.Bytes(),
			Nonce:      nonce[:],
			PartialSig: partialSig,
		},
	)

	return err
}
