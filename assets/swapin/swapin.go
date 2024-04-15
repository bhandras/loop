package swapin

import (
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/loop/assets/htlc"
	"github.com/lightninglabs/loop/fsm"
	"github.com/lightninglabs/taproot-assets/proof"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
)

// SwapIn is a struct that represents a swap initiated by a client to buy an
// asset from the server in return for a lightning (bitcoin) payment.
type SwapIn struct {
	*htlc.SwapKit

	// InitiationHeight is the height at which the swap was initiated.
	InitiationHeight int32

	// Expiry is the block height at which the swap will expire.
	Expiry int64

	// PrepayInvoice is the HOLD invoice the client has to pay to initiate
	// the swap. It'll be repaid in full if the swap is successful.
	PrepayInvoice string

	// ClientKeyDesc is the key descriptor of the client for the joint key
	// that will be used to create the HTLC.
	ClientKeyDesc *keychain.KeyDescriptor

	// State is the current state of the swap.
	State fsm.StateType

	// HtlcOutPoint is the outpoint of the htlc that was created to
	// perform the swap.
	HtlcOutPoint *wire.OutPoint

	// HtlcConfirmationHeight is the height at which the htlc was
	// confirmed.
	HtlcConfirmationHeight uint32

	// HtlcPkscript is the pkscript of the htlc that was created to
	// perform the swap.
	HtlcPkscript []byte

	// HtlcProof is the proof the asset transfer to the on-chain HTLC.
	HtlcProof *proof.Proof

	// TaprootAssetRoot is the root of the taproot script tree that
	// commits to the asset.
	TaprootAssetRoot chainhash.Hash

	// SwapPreimage is the preimage of the swap hash. It is revealed by the
	// client when they claim the swap payment.
	SwapPreimage lntypes.Preimage

	// LastQuote is the last quote the client received for the swap.
	LastQuote *Quote
}

// SwapInQuote is a struct that represents a quote for a swap in.
type Quote struct {
	// SatsPerAssetUnit is the price of the asset in satoshis.
	SatsPerAssetUnit btcutil.Amount

	// PrepayAmount is the amount the client has to pay to initiate the
	// swap. The prepay is an ordinary lightning payment that will only be
	// settled if the client prevents the swap from completion.
	PrepayAmount btcutil.Amount

	// Expiry is the time at which the quote expires.
	Expiry time.Time
}
