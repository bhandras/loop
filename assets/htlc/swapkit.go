package htlc

import (
	"context"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightninglabs/taproot-assets/commitment"
	"github.com/lightninglabs/taproot-assets/proof"
	"github.com/lightninglabs/taproot-assets/tappsbt"
	"github.com/lightninglabs/taproot-assets/tapscript"
	"github.com/lightninglabs/taproot-assets/tapsend"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
)

// SwapKit is a struct that represents a swap initiated by a client to buy an
// asset from the server in return for a lightning (bitcoin) payment.
type SwapKit struct {
	// SenderPubKey is the public key of the sender for the joint key
	// that will be used to create the HTLC.
	SenderPubKey *btcec.PublicKey

	// ReceiverPubKey is the public key of the receiver that will be used to
	// create the HTLC.
	ReceiverPubKey *btcec.PublicKey

	AssetID []byte

	Amount btcutil.Amount

	SwapHash lntypes.Hash

	CsvExpiry uint32
}

// GetSuccesScript returns the success path script of the swap.
func (s *SwapKit) GetSuccesScript() ([]byte, error) {
	return GenSuccessPathScript(s.ReceiverPubKey, s.SwapHash)
}

// GetTimeoutScript returns the timeout path script of the swap.
func (s *SwapKit) GetTimeoutScript() ([]byte, error) {
	return GenTimeoutPathScript(s.SenderPubKey, int64(s.CsvExpiry))
}

// getAggregateKey returns the aggregate musig2 key of the swap.
func (s *SwapKit) GetAggregateKey() (*btcec.PublicKey, error) {
	aggregateKey, err := input.MuSig2CombineKeys(
		input.MuSig2Version100RC2,
		[]*btcec.PublicKey{
			s.SenderPubKey, s.ReceiverPubKey,
		},
		true,
		&input.MuSig2Tweaks{},
	)
	if err != nil {
		return nil, err
	}

	return aggregateKey.PreTweakedKey, nil
}

// GetTimeOutLeaf returns the timeout leaf of the swap.
func (s *SwapKit) GetTimeOutLeaf() (txscript.TapLeaf, error) {
	timeoutScript, err := s.GetTimeoutScript()
	if err != nil {
		return txscript.TapLeaf{}, err
	}

	timeoutLeaf := txscript.NewBaseTapLeaf(timeoutScript)

	return timeoutLeaf, nil
}

// GetSuccessLeaf returns the success leaf of the swap.
func (s *SwapKit) GetSuccessLeaf() (txscript.TapLeaf, error) {
	successScript, err := s.GetSuccesScript()
	if err != nil {
		return txscript.TapLeaf{}, err
	}

	successLeaf := txscript.NewBaseTapLeaf(successScript)

	return successLeaf, nil
}

// getSiblingPreimage returns the sibling preimage of the htlc bitcoin toplevel
// output.
func (s *SwapKit) GetSiblingPreimage() (commitment.TapscriptPreimage, error) {
	timeOutLeaf, err := s.GetTimeOutLeaf()
	if err != nil {
		return commitment.TapscriptPreimage{}, err
	}

	successLeaf, err := s.GetSuccessLeaf()
	if err != nil {
		return commitment.TapscriptPreimage{}, err
	}

	branch := txscript.NewTapBranch(timeOutLeaf, successLeaf)

	siblingPreimage := commitment.NewPreimageFromBranch(branch)

	return siblingPreimage, nil
}

// CreateHtlcVpkt creates the vpacket for the htlc.
func (s *SwapKit) CreateHtlcVpkt() (*tappsbt.VPacket, error) {
	assetId := asset.ID{}
	copy(assetId[:], s.AssetID)

	btcInternalKey, err := s.GetAggregateKey()
	if err != nil {
		return nil, err
	}

	siblingPreimage, err := s.GetSiblingPreimage()
	if err != nil {
		return nil, err
	}

	tapScriptKey, _, _, _, err := CreateOpTrueLeaf()
	if err != nil {
		return nil, err
	}

	pkt := &tappsbt.VPacket{
		Inputs: []*tappsbt.VInput{{
			PrevID: asset.PrevID{
				ID: assetId,
			},
		}},
		Outputs:     make([]*tappsbt.VOutput, 0, 2),
		ChainParams: &address.RegressionNetTap,
	}
	pkt.Outputs = append(pkt.Outputs, &tappsbt.VOutput{
		Amount:            0,
		Type:              tappsbt.TypeSplitRoot,
		AnchorOutputIndex: 0,
		ScriptKey:         asset.NUMSScriptKey,
	})
	pkt.Outputs = append(pkt.Outputs, &tappsbt.VOutput{
		// todo(sputn1ck) assetversion
		AssetVersion:      asset.Version(1),
		Amount:            uint64(s.Amount),
		Interactive:       true,
		AnchorOutputIndex: 1,
		ScriptKey: asset.NewScriptKey(
			tapScriptKey.PubKey,
		),
		AnchorOutputInternalKey:      btcInternalKey,
		AnchorOutputTapscriptSibling: &siblingPreimage,
	})

	return pkt, nil
}

// CreateSweepVpkt creates the vpacket for the sweep.
func (s *SwapKit) CreateSweepVpkt(ctx context.Context, htlcProof *proof.Proof,
	scriptKey asset.ScriptKey, internalKey keychain.KeyDescriptor,
) (*tappsbt.VPacket, error) {

	sweepVpkt, err := tappsbt.FromProofs(
		[]*proof.Proof{htlcProof}, &address.RegressionNetTap,
	)
	if err != nil {
		return nil, err
	}
	sweepVpkt.Outputs = append(sweepVpkt.Outputs, &tappsbt.VOutput{
		AssetVersion:            asset.Version(1),
		Amount:                  uint64(s.Amount),
		Interactive:             true,
		AnchorOutputIndex:       0,
		ScriptKey:               scriptKey,
		AnchorOutputInternalKey: internalKey.PubKey,
	})
	sweepVpkt.Outputs[0].SetAnchorInternalKey(
		internalKey, address.RegressionNetTap.HDCoinType,
	)

	err = tapsend.PrepareOutputAssets(ctx, sweepVpkt)
	if err != nil {
		return nil, err
	}

	_, _, _, controlBlock, err := CreateOpTrueLeaf()
	if err != nil {
		return nil, err
	}

	controlBlockBytes, err := controlBlock.ToBytes()
	if err != nil {
		return nil, err
	}

	opTrueScript, err := GetOpTrueScript()
	if err != nil {
		return nil, err
	}

	witness := wire.TxWitness{
		opTrueScript,
		controlBlockBytes,
	}
	firstPrevWitness := &sweepVpkt.Outputs[0].Asset.PrevWitnesses[0]
	if sweepVpkt.Outputs[0].Asset.HasSplitCommitmentWitness() {
		rootAsset := firstPrevWitness.SplitCommitment.RootAsset
		firstPrevWitness = &rootAsset.PrevWitnesses[0]
	}
	firstPrevWitness.TxWitness = witness

	return sweepVpkt, nil
}

// genTimeoutBtcControlBlock generates the control block for the timeout path of
// the swap.
func (s *SwapKit) GenTimeoutBtcControlBlock(taprootAssetRoot []byte) (
	*txscript.ControlBlock, error) {

	internalKey, err := s.GetAggregateKey()
	if err != nil {
		return nil, err
	}

	successLeaf, err := s.GetSuccessLeaf()
	if err != nil {
		return nil, err
	}

	successLeafHash := successLeaf.TapHash()

	btcControlBlock := &txscript.ControlBlock{
		LeafVersion:    txscript.BaseLeafVersion,
		InternalKey:    internalKey,
		InclusionProof: append(successLeafHash[:], taprootAssetRoot[:]...),
	}

	timeoutPathScript, err := s.GetTimeoutScript()
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

// genSuccessBtcControlBlock generates the control block for the timeout path of
// the swap.
func (s *SwapKit) GenSuccessBtcControlBlock(taprootAssetRoot []byte) (
	*txscript.ControlBlock, error) {

	internalKey, err := s.GetAggregateKey()
	if err != nil {
		return nil, err
	}

	timeOutLeaf, err := s.GetTimeOutLeaf()
	if err != nil {
		return nil, err
	}

	timeOutLeafHash := timeOutLeaf.TapHash()

	btcControlBlock := &txscript.ControlBlock{
		LeafVersion:    txscript.BaseLeafVersion,
		InternalKey:    internalKey,
		InclusionProof: append(timeOutLeafHash[:], taprootAssetRoot[:]...),
	}

	successPathScript, err := s.GetSuccesScript()
	if err != nil {
		return nil, err
	}

	rootHash := btcControlBlock.RootHash(successPathScript)
	tapKey := txscript.ComputeTaprootOutputKey(internalKey, rootHash)
	if tapKey.SerializeCompressed()[0] ==
		secp256k1.PubKeyFormatCompressedOdd {

		btcControlBlock.OutputKeyYIsOdd = true
	}

	return btcControlBlock, nil
}

// genTaprootAssetRootFromProof generates the taproot asset root from the proof
// of the swap.
func GenTaprootAssetRootFromProof(proof *proof.Proof) ([]byte, error) {
	assetCpy := proof.Asset.Copy()
	assetCpy.PrevWitnesses[0].SplitCommitment = nil
	sendCommitment, err := commitment.NewAssetCommitment(
		assetCpy,
	)
	if err != nil {
		return nil, err
	}

	assetCommitment, err := commitment.NewTapCommitment(
		sendCommitment,
	)
	if err != nil {
		return nil, err
	}
	taprootAssetRoot := txscript.AssembleTaprootScriptTree(
		assetCommitment.TapLeaf(),
	).RootNode.TapHash()

	return taprootAssetRoot[:], nil
}

// GetPkScriptFromAsset returns the toplevel bitcoin script with the given
// asset.
func (s *SwapKit) GetPkScriptFromAsset(asset *asset.Asset) ([]byte, error) {
	assetCpy := asset.Copy()
	assetCpy.PrevWitnesses[0].SplitCommitment = nil
	sendCommitment, err := commitment.NewAssetCommitment(
		assetCpy,
	)
	if err != nil {
		return nil, err
	}

	assetCommitment, err := commitment.NewTapCommitment(
		sendCommitment,
	)
	if err != nil {
		return nil, err
	}

	siblingPreimage, err := s.GetSiblingPreimage()
	if err != nil {
		return nil, err
	}

	siblingHash, err := siblingPreimage.TapHash()
	if err != nil {
		return nil, err
	}

	btcInternalKey, err := s.GetAggregateKey()
	if err != nil {
		return nil, err
	}

	return tapscript.PayToAddrScript(
		*btcInternalKey, siblingHash, *assetCommitment,
	)
}
