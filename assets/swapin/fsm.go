package swapin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets/htlc"
	"github.com/lightninglabs/loop/fsm"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/taproot-assets/taprpc"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
)

const (
	// Limit the observers transition observation stack to 15 entrief.
	defaultObserverSize = 15

	// clientKeyFamily is the key family used for the client key.
	clientKeyFamily = 696969
)

const (
	// SwapInInit is the initial state of the swap.
	StateInit fsm.StateType = "SwapInStateInit"

	StateBroadcastHtlcTx fsm.StateType = "SwapInStateBroadcastHtlcTx"

	StateWaitForHtlcConfirmation fsm.StateType = "SwapInStateWaitForHtlcConfirmation"

	StateHtlcConfirmed fsm.StateType = "SwapInStateHtlcConfirmed"

	// statePendingQuote is the state where the swap is waiting for a quote.
	StatePendingQuote fsm.StateType = "SwapInPending"

	// stateHasQuote the state where the client has received a valid quote
	// for the swap. The quote may expire eventually.
	StateAcquiredQuote fsm.StateType = "SwapInStateAcquiredQuote"

	StateExecuteSwap fsm.StateType = "SwapInStateExecuteSwap"

	// stateFinished is the state where the swap has finished.
	StateFinished fsm.StateType = "SwapInStateFinished"

	StateSendSwapPayment fsm.StateType = "SwapInStateSendSwapPayment"

	StateRevealProof fsm.StateType = "SwapInStateRevealProof"

	StateSettleSwap fsm.StateType = "SwapInStateSettleSwap"

	// swapInFailedTimeout is the state where the swap has finished due
	// to a timeout.
	StateFailedTimeout fsm.StateType = "SwapInStateFailedTimeout"

	// swapInFailed is the state where the swap has failed.
	StateFailed fsm.StateType = "SwapInStateFailed"

	// swapInFailedNeedAdmin is the state where the swap has failed and
	// we need to alert the admin.
	StateFailedNeedAdmin fsm.StateType = "SwapInStateFailedNeedAdmin"
)

const (
	// EventRequestNew is the event where the client is requesting a new
	// swap in.
	EventOnRequestNew = fsm.EventType("SwapInEventOnRequestNew")

	// EventOnInit is the event where the server has initialized the swap
	// in.
	EventOnInit = fsm.EventType("SwapInEventInit")

	EventOnHtlcTxBroadcast = fsm.EventType("SwapInEventOnHtlcTxBroadcast")

	EventOnHtlcTxConfirmed = fsm.EventType("SwapInEventOnHtlcTxConfirmed")

	EventOnHtlcTimedOut = fsm.EventType("SwapInEventOnHtlcTimedOut")

	EventOnProofReady = fsm.EventType("SwapInEventOnProofReady")

	// EventOnQuote is the event where the client is requesting a quote for
	// the swap.
	EventOnQuote = fsm.EventType("SwapInEventOnQuote")

	EventOnQuoteAcquired = fsm.EventType("SwapInEventOnQuoteAcquired")

	EventOnExecuteSwap = fsm.EventType("SwapInEventOnExecuteSwap")

	EventOnPrepaySent = fsm.EventType("SwapInEventOnPrepaySent")

	EventOnSwapInvoicePaid = fsm.EventType("SwapInEventOnSwapInvoicePaid")

	EventOnProofRevealed = fsm.EventType("SwapInEventOnProofRevealed")

	EventOnSwapSettled = fsm.EventType("SwapInEventOnSwapSettled")

	// swapInEventOnRecover is the event where the swap is being recovered.
	eventOnRecover = fsm.EventType("SwapInEventOnRecover")

	// eventOnFinish is the event where the swap has finished.
	eventOnFinish = fsm.EventType("SwapInEventOnFinish")

	// eventOnTimeout is the event where the swap has finished due
	// to a timeout.
	eventOnTimeout = fsm.EventType("SwapInEventOnTimeout")
)

var (
	// finishedStates is a list of all states that are considered finished.
	finishedStates = []fsm.StateType{
		StateFinished,
		StateFailedTimeout,
		StateFailed,
		StateFailedNeedAdmin,
	}
)

// FSMConfig contains the configuration for the Swap In FSM.
type FSMConfig struct {
	// TapdClient is the client to interact with the taproot asset daemon.
	TapdClient AssetClient

	// AssetClient is the client to interact with the asset swap server.
	AssetClient swapserverrpc.AssetsSwapServerClient

	// BlockHeightSubscriber is the subscriber to the block height.
	BlockHeightSubscriber BlockHeightSubscriber

	// InvoiceSubscriber is the subscriber to the invoice.
	InvoiceSubscriber InvoiceSubscriber

	// TxConfSubscriber is the subscriber to the transaction confirmation.
	TxConfSubscriber TxConfirmationSubscriber

	// ExchangeRateProvider is the provider for the exchange rate.
	ExchangeRateProvider ExchangeRateProvider

	// Invoices is the invoices client.
	Invoices lndclient.InvoicesClient

	// Wallet is the wallet client.
	Wallet lndclient.WalletKitClient

	// Router is the lnd router client.
	Router lndclient.RouterClient

	// Signer is the signer client.
	Signer lndclient.SignerClient

	// Store is the swap store.
	Store SwapStore

	ChainParams *chaincfg.Params
}

type FSM struct {
	*fsm.StateMachine

	runCtx context.Context

	cfg *FSMConfig

	// SwapIn contains all the information about the swap.
	SwapIn *SwapIn

	// PrepayInvoice is the invoice that we use to receive the prepay
	// payment. We only keep this in the FSM struct as we don't need to
	// save it in the store.
	PrepayInvoice string

	// RawHtlcProof is the raw htlc proof that we need to send to the
	// receiver. We only keep this in the FSM struct as we don't want
	// to save it in the store.
	RawHtlcProof []byte

	// SwapInvoice is the invoice that we use to receive the swap payment.
	// We only keep this in the FSM struct as we don't need to save it in
	// the store.
	SwapInvoice string
}

// NewFSM creates a new swap in FSM.
func NewFSM(ctx context.Context, cfg *FSMConfig) *FSM {
	swapIn := &SwapIn{
		State: fsm.EmptyState,
	}

	return NewFSMFromSwap(ctx, cfg, swapIn)
}

// NewFSMFromSwap creates a new FSM from a existing swap.
func NewFSMFromSwap(ctx context.Context, cfg *FSMConfig,
	swap *SwapIn) *FSM {

	swapInFSM := &FSM{
		runCtx: ctx,
		cfg:    cfg,
		SwapIn: swap,
	}

	swapInFSM.StateMachine = fsm.NewStateMachineWithState(
		swapInFSM.GetStates(), swapInFSM.SwapIn.State,
		defaultObserverSize,
	)
	swapInFSM.ActionEntryFunc = swapInFSM.updateSwap

	return swapInFSM
}

// GetStates returns the swap out state machine.
func (f *FSM) GetStates() fsm.States {
	return fsm.States{
		fsm.EmptyState: fsm.State{
			Transitions: fsm.Transitions{
				EventOnRequestNew: StateInit,
			},
			Action: nil,
		},
		StateInit: fsm.State{
			Transitions: fsm.Transitions{
				EventOnInit:    StatePendingQuote,
				eventOnRecover: StateFailed,
				fsm.OnError:    StateFailed,
			},
			Action: f.OnInit,
		},
		StatePendingQuote: fsm.State{
			Transitions: fsm.Transitions{
				EventOnQuoteAcquired: StateAcquiredQuote,
				fsm.OnError:          StateFailed,
			},
			Action: f.Quote,
		},
		StateAcquiredQuote: fsm.State{
			Transitions: fsm.Transitions{
				EventOnQuote:       StatePendingQuote,
				EventOnExecuteSwap: StateExecuteSwap,
				fsm.OnError:        StateFailed,
			},
			Action: fsm.NoOpAction,
		},
		StateExecuteSwap: fsm.State{
			Transitions: fsm.Transitions{
				EventOnPrepaySent: StateSendSwapPayment,
				fsm.OnError:       StateFailed,
			},
			Action: f.beginSwap,
		},
		StateSendSwapPayment: fsm.State{
			Transitions: fsm.Transitions{
				EventOnSwapInvoicePaid: StateBroadcastHtlcTx,
				fsm.OnError:            StateFailed,
			},
			Action: f.waitForSwapPayment,
		},
		StateBroadcastHtlcTx: fsm.State{
			Transitions: fsm.Transitions{
				EventOnHtlcTxBroadcast: StateWaitForHtlcConfirmation,
				fsm.OnError:            StateFailed,
			},
			Action: f.BroadcastHtlcTx,
		},
		StateWaitForHtlcConfirmation: fsm.State{
			Transitions: fsm.Transitions{
				EventOnHtlcTxConfirmed: StateHtlcConfirmed,
				fsm.OnError:            StateFailed,
			},
			Action: f.subscribeToHtlcTxConfirmed,
		},
		StateHtlcConfirmed: fsm.State{
			Transitions: fsm.Transitions{
				EventOnProofReady: StateRevealProof,
				fsm.OnError:       StateFailed,
			},
			Action: f.handleHtlcTxConfirmed,
		},
		StateRevealProof: fsm.State{
			Transitions: fsm.Transitions{
				EventOnProofRevealed: StateSettleSwap,
				fsm.OnError:          StateFailed,
			},
			Action: f.revealProof,
		},
		StateSettleSwap: fsm.State{
			Transitions: fsm.Transitions{
				EventOnSwapSettled: StateFinished,
				fsm.OnError:        StateFailed,
			},
			Action: f.settleSwap,
		},
		StateFinished: fsm.State{
			Action: fsm.NoOpAction,
		},
		StateFailedTimeout: fsm.State{
			Action: fsm.NoOpAction,
		},
		StateFailed: fsm.State{
			Action: fsm.NoOpAction,
		},
		StateFailedNeedAdmin: fsm.State{
			Action: f.alertAdmins,
		},
	}
}

// InitContext is the initial context for the InitSwapIn state.
type InitContext struct {
	// SwapHash is the hash of the swap preimage. It is generated by the
	// sender and used to identify the swap as well as included in the HTLC.
	SwapHash lntypes.Hash

	// AssetID is the asset id the client wants to buy through the swap.
	AssetID []byte

	// Amount is the amount of the asset the client wants to buy.
	Amount uint64

	// SenderPubkey is the pubkey of the client that wants to buy the asset.
	// It is used to create the HTLC.
	SenderPubkey *btcec.PublicKey
}

// Init is the first state of the swap out FSM. It is responsible for
// creating a new swap out and prepay invoice.
func (f *FSM) OnInit(initCtx fsm.EventContext) fsm.EventType {
	// We expect the event context to be of type *InstantOutContext.
	req, ok := initCtx.(*InitContext)
	if !ok {
		f.Errorf("expected InitSwapInContext, got %T", initCtx)
		return f.HandleError(fsm.ErrInvalidContextType)
	}

	// Create a new key for the swap.
	clientKeyDesc, err := f.cfg.Wallet.DeriveNextKey(
		f.runCtx, clientKeyFamily,
	)
	if err != nil {
		return f.HandleError(err)
	}

	initiationHeight := f.cfg.BlockHeightSubscriber.GetBlockHeight()

	res, err := f.cfg.AssetClient.NewAssetLoopIn(
		f.runCtx, &swapserverrpc.NewAssetLoopInRequest{
			AssetId:      req.AssetID,
			Amount:       req.Amount,
			SenderPubkey: clientKeyDesc.PubKey.SerializeCompressed(),
		},
	)
	if err != nil {
		return f.HandleError(err)
	}

	swapHash, err := lntypes.MakeHash(res.SwapHash)
	if err != nil {
		return f.HandleError(err)
	}

	receiverKey, err := btcec.ParsePubKey(res.ReceiverPubkey)
	if err != nil {
		return f.HandleError(err)
	}

	swapIn := &SwapIn{
		SwapKit: &htlc.SwapKit{
			SenderPubKey:   clientKeyDesc.PubKey,
			ReceiverPubKey: receiverKey,
			AssetID:        req.AssetID,
			Amount:         btcutil.Amount(req.Amount),
			SwapHash:       swapHash,
			CsvExpiry:      100, // TODO: make this configurable
		},
		InitiationHeight: initiationHeight,
		State:            StateInit,
	}

	f.SwapIn = swapIn
	err = f.cfg.Store.CreateAssetSwapIn(f.runCtx, swapIn)
	if err != nil {
		return f.HandleError(err)
	}

	return EventOnInit
}

// BroadcastHtlcTx is the state where we broadcast the htlc transaction.
func (f *FSM) BroadcastHtlcTx(_ fsm.EventContext) fsm.EventType {
	// First, we'll check again if we have enough balance.
	err := f.cfg.TapdClient.CheckBalanceById(
		f.runCtx, f.SwapIn.AssetID, btcutil.Amount(f.SwapIn.Amount),
	)
	if err != nil {
		return f.HandleError(err)
	}

	// First we'll create and fund the vpkt.
	vpkt, err := f.SwapIn.CreateHtlcVpkt()
	if err != nil {
		return f.HandleError(err)
	}

	fundedVpkt, err := f.cfg.TapdClient.FundAndSignVpacket(f.runCtx, vpkt)
	if err != nil {
		return f.HandleError(err)
	}

	feeRate, err := f.cfg.Wallet.EstimateFeeRate(
		f.runCtx, defaultHtlcFeeConfTarget,
	)
	if err != nil {
		return f.HandleError(err)
	}

	// We'll now commit the vpkt in the btcpacket.
	btcPacket, activeAssets, passiveAssets, commitResp, err :=
		f.cfg.TapdClient.PrepareAndCommitVirtualPsbts(
			f.runCtx, fundedVpkt, feeRate.FeePerVByte(),
		)
	if err != nil {
		return f.HandleError(err)
	}

	// Now we'll sign and finalize the btc packet.
	signedBtcPacket, err := f.cfg.Wallet.SignPsbt(f.runCtx, btcPacket)
	if err != nil {
		return f.HandleError(err)
	}

	finalizedBtcPacket, _, err := f.cfg.Wallet.FinalizePsbt(
		f.runCtx, signedBtcPacket, "",
	)
	if err != nil {
		return f.HandleError(err)
	}

	// Now we'll publish and log the transfer.
	sendResp, err := f.cfg.TapdClient.LogAndPublish(
		f.runCtx, finalizedBtcPacket, activeAssets, passiveAssets,
		commitResp,
	)
	if err != nil {
		return f.HandleError(err)
	}

	htlcAnchor := sendResp.Transfer.Outputs[1].Anchor
	htlcOutpoint, err := wire.NewOutPointFromString(htlcAnchor.Outpoint)
	if err != nil {
		return f.HandleError(err)
	}

	f.SwapIn.HtlcOutPoint = htlcOutpoint
	pkscript, err := f.SwapIn.GetPkScriptFromAsset(
		fundedVpkt.Outputs[1].Asset,
	)
	if err != nil {
		return f.HandleError(err)
	}
	f.Debugf("pkscript 1: %x", pkscript)

	pkscript = finalizedBtcPacket.UnsignedTx.TxOut[1].PkScript
	f.SwapIn.HtlcPkscript = pkscript
	f.Debugf("pkscript 2: %x", pkscript)

	// TODO: store update
	/*
		// We can now save the swap outpoint.
		err = f.cfg.Store.UpdateAssetSwapHtlcOutpoint(
			f.runCtx, f.SwapIn.SwapHash, htlcOutpoint, 0,
			pkscript,
		)
		if err != nil {
			return f.HandleError(err)
		}
	*/

	return EventOnHtlcTxBroadcast
}

// subscribeToHtlcTxConfirmed is the state where we subscribe to the htlc
// transaction to wait for it to be confirmed.
//
// Todo(sputn1ck): handle rebroadcasting if it doesn't confirm.
func (f *FSM) subscribeToHtlcTxConfirmed(_ fsm.EventContext) fsm.EventType {
	txConfCtx, cancel := context.WithCancel(f.runCtx)

	confCallback := func(conf *chainntnfs.TxConfirmation, err error) {
		if err != nil {
			// TODO: not thread safe
			f.LastActionError = err
			f.SendEvent(fsm.OnError, nil)
		}
		cancel()
		f.Debugf("htlc tx confirmed: %v:%d", conf.Tx.TxHash(),
			conf.TxIndex)
		f.SendEvent(EventOnHtlcTxConfirmed, conf)
	}

	err := f.cfg.TxConfSubscriber.SubscribeTxConfirmation(
		txConfCtx, f.SwapIn.SwapHash,
		&f.SwapIn.HtlcOutPoint.Hash, f.SwapIn.HtlcPkscript,
		defaultHtlcConfRequirement, f.SwapIn.InitiationHeight,
		confCallback,
	)
	if err != nil {
		return f.HandleError(err)
	}

	return fsm.NoOp
}

// handleHtlcTxConfirmed is the state where we handle the confirmation of the
// htlc transaction. If we don't have a proof in memory yet, we'll export it
// from the asset client.
func (f *FSM) handleHtlcTxConfirmed(event fsm.EventContext) fsm.EventType {
	// If we have an EventContext with a confirmation, we'll save the
	// confirmation height.
	if event != nil {
		if conf, ok := event.(*chainntnfs.TxConfirmation); ok {
			f.SwapIn.HtlcConfirmationHeight = conf.BlockHeight
			// TODO: update the swap in the store.
			/*
				err := o.cfg.Store.UpdateAssetSwapHtlcOutpoint(
					o.runCtx, o.SwapOut.SwapPreimage.Hash(),
					o.SwapOut.HtlcOutPoint, int32(conf.BlockHeight),
					o.SwapOut.HtlcPkscript,
				)
				if err != nil {
					o.Errorf(
						"unable to update swap outpoint: %v",
						err,
					)
				}
			*/
		}
	}

	// We'll now subscribe to the expiry of the htlc output.
	callback := func() {
		f.SendEvent(EventOnHtlcTimedOut, nil)
	}
	htlcExpiry := int32(f.SwapIn.HtlcConfirmationHeight +
		f.SwapIn.CsvExpiry)

	alreadyExpired := f.cfg.BlockHeightSubscriber.SubscribeExpiry(
		f.SwapIn.SwapHash, htlcExpiry, callback,
	)
	if alreadyExpired {
		return EventOnHtlcTimedOut
	}

	// If we already have the proof, we can return early.
	// TODO: do we need this?
	if f.RawHtlcProof != nil {
		return EventOnProofReady
	}

	// Now that the htlc transaction has been confirmed, we can extract the
	// proof from our tapd client.
	// Create the proof for the output.
	scriptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return f.HandleError(err)
	}
	htlcProofRes, err := f.cfg.TapdClient.ExportProof(
		f.runCtx, &taprpc.ExportProofRequest{
			AssetId:   f.SwapIn.AssetID,
			ScriptKey: scriptKey.PubKey.SerializeCompressed(),
			Outpoint: &taprpc.OutPoint{
				Txid:        f.SwapIn.HtlcOutPoint.Hash[:],
				OutputIndex: f.SwapIn.HtlcOutPoint.Index,
			},
		},
	)
	if err != nil {
		return f.HandleError(err)
	}

	f.RawHtlcProof = htlcProofRes.RawProofFile

	return EventOnProofReady
}

func (f *FSM) Quote(_ fsm.EventContext) fsm.EventType {
	res, err := f.cfg.AssetClient.QuoteAssetLoopIn(
		f.runCtx, &swapserverrpc.QuoteAssetLoopInRequest{
			SwapHash: f.SwapIn.SwapHash[:],
		},
	)
	if err != nil {
		return f.HandleError(err)
	}

	f.SwapIn.LastQuote = &Quote{
		SatsPerAssetUnit: btcutil.Amount(res.SatsPerAssetUnit),
		PrepayAmount:     btcutil.Amount(res.PrepayAmt),
		Expiry:           time.Unix(res.Expiry, 0).UTC(),
	}

	return fsm.EventType(EventOnQuoteAcquired)
}

func (f *FSM) beginSwap(_ fsm.EventContext) fsm.EventType {
	var swapHash lntypes.Hash
	copy(swapHash[:], f.SwapIn.SwapHash[:])

	swapAmt := f.SwapIn.LastQuote.SatsPerAssetUnit *
		btcutil.Amount(f.SwapIn.Amount)

	swapInvoice, err := f.cfg.Invoices.AddHoldInvoice(
		f.runCtx, &invoicesrpc.AddInvoiceData{
			Memo:   "swap in " + f.SwapIn.SwapHash.String(),
			Hash:   &swapHash,
			Value:  lnwire.NewMSatFromSatoshis(swapAmt),
			Expiry: 3600,
			// TODO: add cltv expiry
			CltvExpiry: 144,
		},
	)
	if err != nil {
		return f.HandleError(err)
	}

	res, err := f.cfg.AssetClient.ExecuteAssetLoopIn(
		f.runCtx, &swapserverrpc.ExecuteAssetLoopInRequest{
			SwapHash:    f.SwapIn.SwapHash[:],
			SwapInvoice: swapInvoice,
		},
	)
	if err != nil {
		return f.HandleError(err)
	}

	prepayInvoice := res.PrepayInvoice
	f.SwapIn.PrepayInvoice = prepayInvoice

	prepayReq, err := zpay32.Decode(prepayInvoice, f.cfg.ChainParams)
	if err != nil {
		f.Errorf("unable to decode prepay invoice: %v", err)
		return f.HandleError(err)
	}

	f.Debugf("Prepay invoice received: %v", prepayInvoice)

	// TODO: Validate prepay invoice.

	// TODO: decide on this.
	const initialStateTimeout = 30 * time.Second
	prepayCtx, cancel := context.WithTimeout(
		f.runCtx, initialStateTimeout,
	)
	defer cancel()

	// Now let's pay the prepary invoice.
	payStatusChan, payErrChan, err := f.cfg.Router.SendPayment(
		prepayCtx, lndclient.SendPaymentRequest{
			Invoice: prepayInvoice,
			Timeout: initialStateTimeout,
			MaxFee:  prepayReq.MilliSat.ToSatoshis(),
		})
	if err != nil {
		return f.HandleError(err)
	}
	fmt.Println("Prepay invoice payment sent")

	select {
	case payStatus := <-payStatusChan:
		f.Infof("prepay invoice status: %v", payStatus.State)

		return EventOnPrepaySent

	case err := <-payErrChan:
		return f.HandleError(err)

	case <-prepayCtx.Done():
	}

	return f.HandleError(fmt.Errorf("prepay invoice timeout"))
}

func (f *FSM) waitForSwapPayment(_ fsm.EventContext) fsm.EventType {
	f.Infof("Waiting for the swap payment: %v", f.SwapIn.SwapHash)

	// Create the payment listener go routine.
	invoiceCtx, cancel := context.WithCancel(f.runCtx)

	callback := func(update lndclient.InvoiceUpdate, err error) {
		f.Debugf("swap invoice update: %v", update)
		if update.State == invoices.ContractCanceled {
			cancel()
			f.Errorf("swap invoice canceled")
			f.SendEvent(fsm.OnError, nil)
		} else if update.State == invoices.ContractAccepted {
			// TOOD: do we need to cancel?
			// cancel()
			f.Infof("swap invoice accepted")
			f.SendEvent(EventOnSwapInvoicePaid, nil)
		}

		if err != nil {
			cancel()
			f.Errorf("prepay invoice error: %v", err)

			// TODO: is this thread safe?
			f.LastActionError = err
			f.SendEvent(fsm.OnError, nil)
		}
	}

	err := f.cfg.InvoiceSubscriber.SubscribeInvoice(
		invoiceCtx, f.SwapIn.SwapHash, callback,
	)
	if err != nil {
		return f.HandleError(err)
	}

	return fsm.NoOp
}

func (f *FSM) revealProof(_ fsm.EventContext) fsm.EventType {
	res, err := f.cfg.AssetClient.RevealOutputProof(f.runCtx,
		&swapserverrpc.RevealOutputProofRequest{
			SwapHash:     f.SwapIn.SwapHash[:],
			RawProofFile: f.RawHtlcProof,
		},
	)
	if err != nil {
		f.Errorf("unable to reveal proof: %v", err)
		return f.HandleError(err)
	}

	preimage, err := lntypes.MakePreimage(res.Preimage)
	if err != nil {
		f.Errorf("unable to parse swap preimage: %v", err)
		return f.HandleError(err)
	}
	f.SwapIn.SwapPreimage = preimage

	return EventOnProofRevealed
}

func isInvoiceAlreadySettled(err error) bool {
	return strings.Contains(err.Error(), "invoice already settled")
}

func (f *FSM) settleSwap(_ fsm.EventContext) fsm.EventType {
	f.Infof("Attempting to settle the swap invoice")
	err := f.cfg.Invoices.SettleInvoice(f.runCtx, f.SwapIn.SwapPreimage)
	if err != nil && !isInvoiceAlreadySettled(err) {
		f.Errorf("unable to settle swap invoice: %v", err)
		return f.HandleError(err)
	}

	return EventOnSwapSettled
}

// updateSwap is called after every action and updates the swap in the db.
func (f *FSM) updateSwap(notification fsm.Notification) {
	f.Infof("Current: %v", notification.NextState)

	// Skip the update if the swap is not yet initialized.
	if f.SwapIn == nil {
		return
	}

	f.SwapIn.State = notification.NextState

	// If we're in the early stages we don't have created the swap in the
	// store yet and won't need to update it.
	if f.SwapIn.State == StateInit {
		return
	}

	err := f.cfg.Store.UpdateAssetSwapIn(f.runCtx, f.SwapIn)
	if err != nil {
		log.Errorf("Error updating swap : %v", err)
		return
	}
}

func (f *FSM) swapHashStr() string {
	if f.SwapIn.SwapKit == nil {
		return "N/A"
	}

	return f.SwapIn.SwapHash.String()
}

// Infof logs an info message with the swap hash as prefix.
func (f *FSM) Infof(format string, args ...interface{}) {
	log.Infof(
		"Swap %v: "+format,
		append(
			[]interface{}{f.swapHashStr()},
			args...,
		)...,
	)
}

// Debugf logs a debug message with the swap hash as prefix.
func (f *FSM) Debugf(format string, args ...interface{}) {
	log.Debugf(
		"Swap %v: "+format,
		append(
			[]interface{}{f.swapHashStr()},
			args...,
		)...,
	)
}

// Errorf logs an error message with the swap hash as prefix.
func (f *FSM) Errorf(format string, args ...interface{}) {
	log.Errorf(
		"Swap %v: "+format,
		append(
			[]interface{}{f.swapHashStr()},
			args...,
		)...,
	)
}

// alertAdmins is the action that is executed when the swap has failed and we
// need to alert the adminf. This should throw a keybase exception.
func (f *FSM) alertAdmins(_ fsm.EventContext) fsm.EventType {
	// Alert the admins that something went wrong.
	log.Errorf("Asset Swap In failed: %v, %w", f.SwapIn.SwapHash,
		f.LastActionError)

	return fsm.NoOp
}

// SwapInFinishedStates returns a string slice of all finished statef.
func SwapInFinishedStates() []string {
	states := make([]string, 0, len(finishedStates))
	for _, s := range finishedStates {
		states = append(states, string(s))
	}

	return states
}
