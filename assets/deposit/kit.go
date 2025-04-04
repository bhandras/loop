package deposit

import (
	"bytes"
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/btcsuite/btcd/btcec/v2/schnorr/musig2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/psbt"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/assets"
	"github.com/lightninglabs/loop/assets/htlc"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightninglabs/taproot-assets/commitment"
	"github.com/lightninglabs/taproot-assets/proof"
	"github.com/lightninglabs/taproot-assets/tappsbt"
	"github.com/lightninglabs/taproot-assets/taprpc"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
)

// Kit is a struct that contains all the information needed to create
// and operator a 2-of-2 MuSig2 asset deposit.
type Kit struct {
	// FunderKey is the key of the funder of the deposit.
	FunderKey *btcec.PublicKey

	// CoSignerKey is the key of the counterparty who is co-signing the
	// deposit funding and spending transactions.
	CoSignerKey *btcec.PublicKey

	// KeyLocator is the locator of either the funders or the co-signers
	// key depending on which side constructs the deposit.
	KeyLocator keychain.KeyLocator

	// AssetID is the identifier of the asset that will be held in the
	// deposit.
	AssetID asset.ID

	// CsvExpiry is the relative timelock in blocks for the timeout path of
	// the deposit.
	CsvExpiry uint32

	// muSig2Key is the aggregate key of the funder and co-signer.
	muSig2Key *musig2.AggregateKey

	// chainParams is the chain parameters of the chain the deposit is
	// being created on.
	chainParams *address.ChainParams
}

// NewKit creates a new deposit kit with the given funder key, co-signer
// key, key locator, asset ID and CSV expiry.
func NewKit(funderKey, coSignerKey *btcec.PublicKey,
	keyLocator keychain.KeyLocator, assetID asset.ID, csvExpiry uint32,
	chainParams *address.ChainParams) (*Kit, error) {

	sortKeys := true
	muSig2Key, err := input.MuSig2CombineKeys(
		input.MuSig2Version100RC2,
		[]*btcec.PublicKey{
			funderKey, coSignerKey,
		},
		sortKeys,
		&input.MuSig2Tweaks{
			TaprootBIP0086Tweak: true,
		},
	)
	if err != nil {
		return nil, err
	}

	return &Kit{
		FunderKey:   funderKey,
		CoSignerKey: coSignerKey,
		KeyLocator:  keyLocator,
		AssetID:     assetID,
		CsvExpiry:   csvExpiry,
		muSig2Key:   muSig2Key,
		chainParams: chainParams,
	}, nil
}

// GenTimeoutPathScript constructs a csv timeout script for the deposit funder.
//
//	<clientKey> OP_CHECKSIGVERIFY <csvExpiry> OP_CHECKSEQUENCEVERIFY
func (d *Kit) GenTimeoutPathScript() ([]byte, error) {
	builder := txscript.NewScriptBuilder()

	builder.AddData(schnorr.SerializePubKey(d.FunderKey))
	builder.AddOp(txscript.OP_CHECKSIGVERIFY)
	builder.AddInt64(int64(d.CsvExpiry))
	builder.AddOp(txscript.OP_CHECKSEQUENCEVERIFY)

	return builder.Script()
}

// genTimeoutPathSiblingPreimage generates the sibling preimage for the timeout
// path of the deposit. The sibling preimage is the preimage of the tap leaf
// that is the timeout path script.
func (d *Kit) genTimeoutPathSiblingPreimage() ([]byte, error) {
	timeoutScript, err := d.GenTimeoutPathScript()
	if err != nil {
		return nil, err
	}

	btcTapLeaf := txscript.TapLeaf{
		LeafVersion: txscript.BaseLeafVersion,
		Script:      timeoutScript,
	}

	siblingPreimage, err := commitment.NewPreimageFromLeaf(btcTapLeaf)
	if err != nil {
		return nil, err
	}

	siblingPreimageBytes, _, err := commitment.MaybeEncodeTapscriptPreimage(
		siblingPreimage,
	)
	if err != nil {
		return nil, err
	}

	return siblingPreimageBytes, nil
}

// NewAddr creates a new deposit address to send funds to. The address is
// created with a MuSig2 key that is a combination of the funder and co-signer
// keys. The resulting anchor output will have a timeout path script that is a
// combination of the funder key and a CSV timelock.
func (d *Kit) NewAddr(ctx context.Context, funder *assets.TapdClient,
	amount uint64) (*taprpc.Addr, error) {

	siblingPreimageBytes, err := d.genTimeoutPathSiblingPreimage()
	if err != nil {
		return nil, err
	}

	tapScriptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return nil, err
	}

	btcInternalKey := d.muSig2Key.PreTweakedKey
	muSig2Addr, err := funder.NewAddr(ctx, &taprpc.NewAddrRequest{
		AssetId:   d.AssetID[:],
		Amt:       amount,
		ScriptKey: taprpc.MarshalScriptKey(tapScriptKey),
		InternalKey: &taprpc.KeyDescriptor{
			RawKeyBytes: btcInternalKey.SerializeCompressed(),
		},
		TapscriptSibling: siblingPreimageBytes,
	})
	if err != nil {
		return nil, err
	}

	return muSig2Addr, nil
}

// IsMatchingAddr checks if the given address is a matching deposit address for
// the deposit kit. It checks that the address has the same internal key, script
// key and sibling preimage as the deposit address. Note that this function does
// not check the amount of the address.
func (d *Kit) IsMatchingAddr(addr string) (bool, error) {
	tap, err := address.DecodeAddress(addr, d.chainParams)
	if err != nil {
		log.Debugf("unable to decode address %v: %v", err)

		return false, err
	}

	tapSciptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return false, err
	}

	keysMatch := tap.InternalKey.IsEqual(d.muSig2Key.PreTweakedKey) &&
		tap.ScriptKey.IsEqual(tapSciptKey.PubKey)

	siblingPreimage1, err := d.genTimeoutPathSiblingPreimage()
	if err != nil {
		return false, err
	}

	siblingPreimage2, _, err := commitment.MaybeEncodeTapscriptPreimage(
		tap.TapscriptSibling,
	)
	if err != nil {
		return false, err
	}

	return keysMatch && bytes.Equal(siblingPreimage1, siblingPreimage2), nil
}

// IsDepositFunded checks if the deposit is funded with the expected amount. It
// does so by checking if there is a deposit output with the expected keys and
// amount in the list of transfers of the funder.
func (d *Kit) IsDepositFunded(ctx context.Context,
	funder *assets.TapdClient, expectedAmount uint64) (bool,
	*taprpc.AssetTransfer, int, error) {

	res, err := funder.ListTransfers(ctx, &taprpc.ListTransfersRequest{})
	if err != nil {
		return false, nil, 0, err
	}

	// Prepare the tap scriptkey for the deposit.
	tapSciptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return false, nil, 0, err
	}
	scriptKey := tapSciptKey.PubKey.SerializeCompressed()
	scriptKey[0] = secp256k1.PubKeyFormatCompressedEven

	// Prepare the sibling preimage for the deposit.
	siblingPreimage, err := d.genTimeoutPathSiblingPreimage()
	if err != nil {
		return false, nil, 0, err
	}

	internalKey := d.muSig2Key.PreTweakedKey.SerializeCompressed()

	// Now iterate over all the transfers to find the deposit.
	// TODO(bhandras): this is inefficient and should be improved.
	for _, transfer := range res.Transfers {
		for outIndex, out := range transfer.Outputs {
			// First make sure that the script key matches.
			if !bytes.Equal(out.ScriptKey, scriptKey) {
				continue
			}

			// Make sure that the internal key also matches.
			if !bytes.Equal(out.Anchor.InternalKey, internalKey) {
				continue
			}

			// Double check that the sibling preimage also matches.
			if !bytes.Equal(out.Anchor.TapscriptSibling,
				siblingPreimage) {

				continue
			}

			// Make sure the amount is as expected.
			if out.Amount == expectedAmount {
				return true, transfer, outIndex, nil
			}
		}
	}

	return false, nil, 0, nil
}

// NewHtlcAddr creates a new HTLC address with the same keys as the deposit.
// This is useful when we're creating an HTLC transaction spending the deposit.
func (d *Kit) NewHtlcAddr(ctx context.Context,
	tapClient *assets.TapdClient, amount uint64, swapHash lntypes.Hash,
	csvExpiry uint32) (*taprpc.Addr, *htlc.SwapKit, error) {

	s := htlc.SwapKit{
		SenderPubKey:   d.FunderKey,
		ReceiverPubKey: d.CoSignerKey,
		AssetID:        d.AssetID[:],
		Amount:         btcutil.Amount(amount),
		SwapHash:       swapHash,
		CsvExpiry:      csvExpiry,
	}

	btcInternalKey, err := s.GetAggregateKey()
	if err != nil {
		return nil, nil, err
	}

	siblingPreimage, err := s.GetSiblingPreimage()
	if err != nil {
		return nil, nil, err
	}

	siblingPreimageBytes, _, err := commitment.MaybeEncodeTapscriptPreimage(
		&siblingPreimage,
	)
	if err != nil {
		return nil, nil, err
	}

	tapScriptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return nil, nil, err
	}

	htlcAddr, err := tapClient.NewAddr(ctx, &taprpc.NewAddrRequest{
		AssetId:   d.AssetID[:],
		Amt:       amount,
		ScriptKey: taprpc.MarshalScriptKey(tapScriptKey),
		InternalKey: &taprpc.KeyDescriptor{
			RawKeyBytes: btcInternalKey.SerializeCompressed(),
		},
		TapscriptSibling: siblingPreimageBytes,
	})
	if err != nil {
		return nil, nil, err
	}

	return htlcAddr, &s, nil
}

// TapScriptKey generates a TAP script-key (the key of the script locking the
// asset) for the deposit.
func (d *Kit) TapScriptKey() (asset.ScriptKey, error) {
	tapScriptKey, _, _, _, err := htlc.CreateOpTrueLeaf()
	if err != nil {
		return asset.ScriptKey{}, err
	}

	return asset.NewScriptKey(tapScriptKey.PubKey), nil
}

// ExportProof exports a proof for the deposit outpoint. The proof is used to
// prove that the deposit is valid and indeed happened.
func (d *Kit) ExportProof(ctx context.Context, funder *assets.TapdClient,
	outpoint *wire.OutPoint) (*taprpc.ProofFile, error) {

	scriptKey, err := d.TapScriptKey()
	if err != nil {
		return nil, err
	}

	return funder.ExportProof(
		ctx, &taprpc.ExportProofRequest{
			AssetId:   d.AssetID[:],
			ScriptKey: scriptKey.PubKey.SerializeCompressed(),
			Outpoint: &taprpc.OutPoint{
				Txid:        outpoint.Hash[:],
				OutputIndex: outpoint.Index,
			},
		},
	)
}

// VerifyProof verifies that the given deposit proof is valid for the deposit
// address. It checks that the internal key of the proof matches the internal
// key of the deposit address and that the sibling preimage of the proof matches
// the sibling preimage of the deposit address. Returns the root hash of the
// anchor output if the proof is valid.
func (d *Kit) VerifyProof(depositProof *proof.Proof) ([]byte, error) {
	// First generate a vpacket from the deposit proof.
	proofVpacket, err := tappsbt.FromProofs(
		[]*proof.Proof{depositProof}, d.chainParams, tappsbt.V1,
	)
	if err != nil {
		return nil, err
	}

	// Now verify that the proof is indeed for the deposit address.
	input := proofVpacket.Inputs[0]

	// First check that the internal key of the proof matches the internal
	// key of the deposit address.
	anchorInternalKeyBytes := input.Anchor.InternalKey.SerializeCompressed()
	depositInternalKey := d.muSig2Key.PreTweakedKey.SerializeCompressed()

	if !bytes.Equal(depositInternalKey, anchorInternalKeyBytes) {
		return nil, fmt.Errorf("internal key mismatch")
	}

	// Next check that the sibling preimage of the proof matches the sibling
	// preimage of the deposit address.
	depositSiblingPreimage, err := d.genTimeoutPathSiblingPreimage()
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(depositSiblingPreimage, input.Anchor.TapscriptSibling) {
		return nil, fmt.Errorf("sibling preimage mismatch")
	}

	return proofVpacket.Inputs[0].Anchor.MerkleRoot, nil
}

// GenTimeoutBtcControlBlock generates the control block for the timeout path of
// the deposit.
func (d *Kit) GenTimeoutBtcControlBlock(taprootAssetRoot []byte) (
	*txscript.ControlBlock, error) {

	internalKey := d.muSig2Key.PreTweakedKey

	btcControlBlock := &txscript.ControlBlock{
		InternalKey:    internalKey,
		LeafVersion:    txscript.BaseLeafVersion,
		InclusionProof: taprootAssetRoot,
	}

	timeoutPathScript, err := d.GenTimeoutPathScript()
	if err != nil {
		return nil, err
	}

	rootHash := btcControlBlock.RootHash(timeoutPathScript)
	tapKey := txscript.ComputeTaprootOutputKey(internalKey, rootHash)
	if tapKey.SerializeCompressed()[0] ==
		secp256k1.PubKeyFormatCompressedOdd {

		btcControlBlock.OutputKeyYIsOdd = true
	}

	return btcControlBlock, nil
}

// CreateTimeoutWitness creates a timeout witness for the deposit.
func (d *Kit) CreateTimeoutWitness(ctx context.Context,
	signer lndclient.SignerClient, depositProof *proof.Proof,
	sweepBtcPacket *psbt.Packet, keyLocator keychain.KeyLocator) (
	wire.TxWitness, error) {

	assetTxOut := sweepBtcPacket.Inputs[0].WitnessUtxo
	feeTxOut := sweepBtcPacket.Inputs[1].WitnessUtxo
	sweepBtcPacket.UnsignedTx.TxIn[0].Sequence = d.CsvExpiry

	timeoutScript, err := d.GenTimeoutPathScript()
	if err != nil {
		return nil, err
	}

	signDesc := &lndclient.SignDescriptor{
		KeyDesc: keychain.KeyDescriptor{
			KeyLocator: keyLocator,
		},
		SignMethod:    input.TaprootScriptSpendSignMethod,
		WitnessScript: timeoutScript,
		Output:        assetTxOut,
		InputIndex:    0,
	}
	rawSigs, err := signer.SignOutputRaw(
		ctx, sweepBtcPacket.UnsignedTx,
		[]*lndclient.SignDescriptor{
			signDesc,
		},
		[]*wire.TxOut{
			assetTxOut, feeTxOut,
		},
	)
	if err != nil {
		return nil, err
	}

	taprootAssetRoot, err := assets.GenTaprootAssetRootFromProof(
		depositProof,
	)
	if err != nil {
		return nil, err
	}

	timeoutControlBlock, err := d.GenTimeoutBtcControlBlock(
		taprootAssetRoot,
	)
	if err != nil {
		return nil, err
	}

	controlBlockBytes, err := timeoutControlBlock.ToBytes()
	if err != nil {
		return nil, err
	}

	return wire.TxWitness{
		rawSigs[0],
		timeoutScript,
		controlBlockBytes,
	}, nil
}
