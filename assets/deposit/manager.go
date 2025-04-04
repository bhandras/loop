package deposit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil/psbt"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightninglabs/taproot-assets/proof"
	"github.com/lightninglabs/taproot-assets/taprpc"
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

	// StatePending indicates that the deposit is pending confirmation on
	// the blockchain.
	StatePending State = 1

	// StateConfirmed indicates that the deposit has been confirmed on the
	// blockchain.
	StateConfirmed State = 2

	// StateExpired indicates that the deposit has expired.
	StateExpired State = 3

	// StateSpent indicates that the deposit has been spent.
	StateSpent State = 4

	// StateWithdrawn indicates that the deposit has been withdrawn.
	StateWithdrawn State = 5
)

func (s State) String() string {
	switch s {
	case StateInitiated:
		return "Initiated"

	case StatePending:
		return "Pending"

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

	// ConfirmationHeight is the block height at which the deposit was
	// confirmed.
	ConfirmationHeight uint32

	// Outpoint is the confirmed deposit outpoint.
	Outpoint string

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
	// Set the current height to the one passed in.
	m.currentHeight = currentHeight

	ctxc, cancel := context.WithCancel(ctx)
	defer func() {
		// Signal to the main event loop that it should stop.
		close(m.quit)

		cancel()
	}()

	// Fetch all active deposits from the store to kick-off the manager.
	activeDeposits, err := m.store.GetActiveDeposits(ctx)
	if err != nil {
		log.Errorf("Unable to fetch deposits from store: %v", err)

		return err
	}

	for i := range activeDeposits {
		d := &activeDeposits[i]
		log.Infof("Recovering deposit %v (state=%s)", d.ID, d.State)

		// If the deposit has just been initiated, then we need to
		// ensure that it is funded.
		switch d.State {
		case StateInitiated:
			m.pendingDeposits[d.ID] = d
			err = m.fundDepositIfNeeded(ctxc, d)
			if err != nil {
				log.Errorf("Unable to fund deposit %v: %v",
					d.ID, err)

				return err
			}
		case StateConfirmed:
			m.activeDeposits[d.ID] = d

		case StateExpired:
			m.expiredDeposits[d.ID] = d
		}
	}

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
			err := m.handleBlockEpoch(ctxc, uint32(blockHeight))
			if err != nil {
				return err
			}

		case err := <-errChan:
			log.Errorf("received error from block epoch "+
				"notification: %v", err)

			return err

		case <-time.After(time.Second * 10):
			for _, d := range m.expiredDeposits {
				log.Infof("Publishing timeout sweep for "+
					"deposit %v", d.ID)

				err := m.publishTimeoutSweep(ctxc, d)
				if err != nil {
					log.Errorf("Unable to publish timeout "+
						"sweep for deposit %v: %v",
						d.ID, err)
				}

				// delete(m.expiredDeposits, d.ID)
			}

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

func (m *Manager) fundDepositIfNeeded(ctx context.Context, d *Deposit) error {
	// If the deposit is already funded then we don't need to fund it. It's
	// also possible that the deposit is already funded, but the state
	// hasn't been updated yet.
	if d.State != StateInitiated || d.ConfirmationHeight != 0 {
		return nil
	}

	// Now list transfers from tapd and check if the deposit is funded.
	funded, transfer, outIndex, err := d.IsDepositFunded(
		ctx, m.tapClient, d.Amount,
	)
	if err != nil {
		log.Errorf("Unable to check if deposit %v is funded: %v", d.ID,
			err)

		return err
	}

	// The deposit has been funded, update the confirmation height.
	if funded {
		configmed := transfer.AnchorTxBlockHeight != 0
		if configmed {
			log.Infof("Deposit %v is already funded (height=%v)",
				d.ID, transfer.AnchorTxBlockHeight)

			d.ConfirmationHeight = transfer.AnchorTxBlockHeight
			d.Outpoint = transfer.Outputs[outIndex].Anchor.Outpoint
		} else {
			log.Infof("Deposit %v is already funded, but not yet "+
				"confirmed", d.ID)
		}

		return nil
	}

	sendResp, err := m.tapClient.SendAsset(
		ctx, &taprpc.SendAssetRequest{
			TapAddrs: []string{d.Addr},
		},
	)
	if err != nil {
		log.Errorf("Unable to send asset to deposit %v: %v", d.ID, err)

		return err
	}

	log.Infof("Asset to deposit %v funded, anchor txid=%x", d.ID,
		sendResp.Transfer.AnchorTxHash)

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
	// Check if any deposits have been confirmed.
	for _, d := range m.pendingDeposits {
		log.Infof("Checking if deposit %v is funded", d.ID)
		// Now list transfers from tapd and check if the deposit is funded.
		funded, transfer, outIndex, err := d.IsDepositFunded(
			ctx, m.tapClient, d.Amount,
		)
		if err != nil {
			log.Errorf("Unable to check if deposit %v is funded: "+
				"%v", d.ID, err)

			return err
		}

		if !funded {
			log.Debugf("Deposit %v is not funded yet", d.ID)
			continue
		}

		if transfer.AnchorTxBlockHeight != 0 {
			log.Infof("Deposit %v is confirmed at block %v", d.ID,
				transfer.AnchorTxBlockHeight)

			d.ConfirmationHeight = transfer.AnchorTxBlockHeight
			d.Outpoint = transfer.Outputs[outIndex].Anchor.Outpoint
			d.State = StateConfirmed

			m.store.MarkDepositConfirmed(
				ctx, d.ID, d.Outpoint, d.ConfirmationHeight,
			)

			// Move the deposit to the active deposits map.
			m.activeDeposits[d.ID] = d
			delete(m.pendingDeposits, d.ID)
		} else {
			log.Infof("Deposit %v is funded, but not yet "+
				"confirmed", d.ID)
		}
	}

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

	return nil
}

func (m *Manager) publishTimeoutSweep(ctx context.Context, d *Deposit) error {
	// Fetch the deposit proof.
	outpoint, err := wire.NewOutPointFromString(d.Outpoint)
	if err != nil {
		log.Errorf("Unable to parse outpoint %v: %v", d.Outpoint, err)

		return err
	}

	// Fetch the (full) proof of the deposit from tapd.
	rpcProofFile, err := d.ExportProof(ctx, m.tapClient, outpoint)
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

	leasedUtxos, err := m.store.GetLeasedUTXOs(ctx)
	if err != nil {
		log.Errorf("Unable to get leased UTXOs: %v", err)

		return err
	}

	var lockedUtxos []*taprpc.OutPoint
	var pinputs []psbt.PInput
	for _, utxo := range leasedUtxos[d.ID] {
		lockedUtxos = append(lockedUtxos, &taprpc.OutPoint{
			Txid:        utxo.Hash[:],
			OutputIndex: utxo.Index,
		})
	}

	if len(lockedUtxos) != 0 {
		leases, err := m.walletKit.ListLeases(ctx)
		if err != nil {
			log.Errorf("Unable to list leases: %v", err)
		}
		for _, lease := range leases {
			for _, utxo := range leasedUtxos[d.ID] {
				if lease.Outpoint == utxo {
					pinputs = append(pinputs, psbt.PInput{
						WitnessUtxo: &wire.TxOut{
							Value:    int64(lease.Value),
							PkScript: lease.PkScript,
						},
					})
				}
			}
		}
	}

	// By committing the virtual transaction to the BTC template we
	// created, Alice's lnd node will fund the BTC level transaction with
	// an input to pay for the fees (and it will also add a change output).
	timeoutSweepBtcPkt, activeAssets, passiveAssets, commitResp, err :=
		m.tapClient.PrepareAndCommitVirtualPsbts(
			ctx, sweepVpkt, feeRate.FeePerVByte(), nil,
			m.addressParams.Params, lockedUtxos, pinputs,
		)
	if err != nil {
		log.Errorf("Unable to prepare and commit virtual psbt: %v",
			err)
	}

	if commitResp != nil && commitResp.LndLockedUtxos != nil {
		// If we have locked UTXOs, then we need to store them so
		// on an eventual restart we can use them to create the
		// timeout sweep witness.
		for _, utxo := range commitResp.LndLockedUtxos {
			op := wire.OutPoint{
				Hash:  chainhash.Hash(utxo.Txid),
				Index: utxo.OutputIndex,
			}

			err = m.store.AssetDepositLeaseUTXO(ctx, d.ID, op)
			if err != nil {
				log.Errorf("Unable to lease UTXO %v: %v",
					op, err)

				return err
			}
		}
	}

	fmt.Printf("Timeout sweep packet: %v\n", spew.Sdump(timeoutSweepBtcPkt))

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
				Txid:        outpoint.Hash[:],
				OutputIndex: outpoint.Index,
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

	fmt.Printf("All deposits: %v\n", spew.Sdump(deposits))

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
