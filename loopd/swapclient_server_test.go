package loopd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btclog/v2"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop"
	"github.com/lightninglabs/loop/labels"
	"github.com/lightninglabs/loop/loopdb"
	"github.com/lightninglabs/loop/looprpc"
	"github.com/lightninglabs/loop/swap"
	mock_lnd "github.com/lightninglabs/loop/test"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/stretchr/testify/require"
)

var (
	testnetAddr, _ = btcutil.NewAddressScriptHash(
		[]byte{123}, &chaincfg.TestNet3Params,
	)

	mainnetAddr, _ = btcutil.NewAddressScriptHash(
		[]byte{123}, &chaincfg.MainNetParams,
	)

	nodepubkeyAddr, _ = btcutil.DecodeAddress(
		mock_lnd.NewMockLnd().NodePubkey, &chaincfg.MainNetParams,
	)

	chanID1 = lnwire.NewShortChanIDFromInt(1)
	chanID2 = lnwire.NewShortChanIDFromInt(2)
	chanID3 = lnwire.NewShortChanIDFromInt(3)
	chanID4 = lnwire.NewShortChanIDFromInt(4)

	peer1 = route.Vertex{1}
	peer2 = route.Vertex{2}

	channel1 = lndclient.ChannelInfo{
		Active:        false,
		ChannelID:     chanID1.ToUint64(),
		PubKeyBytes:   peer1,
		LocalBalance:  10000,
		RemoteBalance: 0,
		Capacity:      10000,
	}

	channel2 = lndclient.ChannelInfo{
		Active:        true,
		ChannelID:     chanID2.ToUint64(),
		PubKeyBytes:   peer2,
		LocalBalance:  10000,
		RemoteBalance: 0,
		Capacity:      10000,
	}

	channel3 = lndclient.ChannelInfo{
		Active:        true,
		ChannelID:     chanID3.ToUint64(),
		PubKeyBytes:   peer2,
		LocalBalance:  10000,
		RemoteBalance: 0,
		Capacity:      10000,
	}

	channel4 = lndclient.ChannelInfo{
		Active:        true,
		ChannelID:     chanID4.ToUint64(),
		PubKeyBytes:   peer2,
		LocalBalance:  1000,
		RemoteBalance: 0,
		Capacity:      1000,
	}
)

// TestValidateConfTarget tests all failure and success cases for our conf
// target validation function, including the case where we replace a zero
// target with the default provided.
func TestValidateConfTarget(t *testing.T) {
	const (
		// Various input confirmation values for tests.
		zeroConf int32 = 0
		oneConf  int32 = 1
		twoConf  int32 = 2
		fiveConf int32 = 5

		// defaultConf is the default confirmation target we use for
		// all tests.
		defaultConf = 6
	)

	tests := []struct {
		name           string
		confTarget     int32
		expectedTarget int32
		expectErr      bool
	}{
		{
			name:           "zero conf, get default",
			confTarget:     zeroConf,
			expectedTarget: defaultConf,
			expectErr:      false,
		},
		{
			name:       "one conf, get error",
			confTarget: oneConf,
			expectErr:  true,
		},
		{
			name:           "two conf, ok",
			confTarget:     twoConf,
			expectedTarget: twoConf,
			expectErr:      false,
		},
		{
			name:           "five conf, ok",
			confTarget:     fiveConf,
			expectedTarget: fiveConf,
			expectErr:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target, err := validateConfTarget(
				test.confTarget, defaultConf,
			)

			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.expectedTarget, target)
		})
	}
}

// TestValidateLoopInRequest tests validation of loop in requests.
func TestValidateLoopInRequest(t *testing.T) {
	tests := []struct {
		name           string
		amount         int64
		numDeposits    uint32
		external       bool
		confTarget     int32
		expectErr      bool
		expectedTarget int32
	}{
		{
			name:           "external and htlc conf set",
			amount:         100_000,
			external:       true,
			confTarget:     1,
			expectErr:      true,
			expectedTarget: 0,
		},
		{
			name:           "external and no conf",
			amount:         100_000,
			external:       true,
			confTarget:     0,
			expectErr:      false,
			expectedTarget: 0,
		},
		{
			name:           "not external, zero conf",
			amount:         100_000,
			external:       false,
			confTarget:     0,
			expectErr:      false,
			expectedTarget: loop.DefaultHtlcConfTarget,
		},
		{
			name:           "not external, bad conf",
			amount:         100_000,
			external:       false,
			confTarget:     1,
			expectErr:      true,
			expectedTarget: 0,
		},
		{
			name:           "not external, ok conf",
			amount:         100_000,
			external:       false,
			confTarget:     5,
			expectErr:      false,
			expectedTarget: 5,
		},
		{
			name:           "not external, amount no deposit",
			amount:         100_000,
			numDeposits:    0,
			external:       false,
			expectErr:      false,
			expectedTarget: loop.DefaultHtlcConfTarget,
		},
		{
			name:        "not external, deposit no amount",
			amount:      100_000,
			numDeposits: 1,
			external:    false,
			expectErr:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			external := test.external
			conf, err := validateLoopInRequest(
				test.confTarget, external, test.numDeposits,
				test.amount,
			)

			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.expectedTarget, conf)
		})
	}
}

// TestValidateLoopOutRequest tests validation of loop out requests.
func TestValidateLoopOutRequest(t *testing.T) {
	tests := []struct {
		name            string
		chain           chaincfg.Params
		confTarget      int32
		destAddr        btcutil.Address
		label           string
		channels        []lndclient.ChannelInfo
		outgoingChanSet []uint64
		amount          int64
		maxRoutingFee   int64
		maxParts        uint32
		err             error
		expectedTarget  int32
	}{
		{
			name:       "mainnet address with mainnet backend",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            nil,
			expectedTarget: 2,
		},
		{
			name:       "mainnet address with testnet backend",
			chain:      chaincfg.TestNet3Params,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            errIncorrectChain,
			expectedTarget: 0,
		},
		{
			name:       "testnet address with testnet backend",
			chain:      chaincfg.TestNet3Params,
			destAddr:   testnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            nil,
			expectedTarget: 2,
		},
		{
			name:       "testnet address with mainnet backend",
			chain:      chaincfg.MainNetParams,
			destAddr:   testnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            errIncorrectChain,
			expectedTarget: 0,
		},
		{
			name:       "invalid label",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      labels.Reserved,
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            labels.ErrReservedPrefix,
			expectedTarget: 0,
		},
		{
			name:       "invalid conf target",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 1,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            errConfTargetTooLow,
			expectedTarget: 0,
		},
		{
			name:       "default conf target",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 0,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxParts:       5,
			err:            nil,
			expectedTarget: 9,
		},
		{
			name:       "valid amount for default channel set",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel1, channel2, channel3,
			},
			amount:         20000,
			maxParts:       5,
			err:            nil,
			expectedTarget: 2,
		},
		{
			name:       "invalid amount for default channel set",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel1, channel2, channel3,
			},
			amount:         25000,
			maxParts:       5,
			err:            errBalanceTooLow,
			expectedTarget: 0,
		},
		{
			name:       "inactive channel in outgoing channel set",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel1, channel2, channel3,
			},
			outgoingChanSet: []uint64{
				chanID1.ToUint64(),
			},
			amount:         1000,
			maxParts:       5,
			err:            errBalanceTooLow,
			expectedTarget: 0,
		},
		{
			name:       "outgoing channel set balance is enough",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel1, channel2, channel3,
			},
			outgoingChanSet: []uint64{
				chanID2.ToUint64(),
			},
			amount:         1000,
			maxParts:       5,
			err:            nil,
			expectedTarget: 2,
		},
		{
			name:       "outgoing channel set balance not sufficient",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel1, channel2, channel3,
			},
			outgoingChanSet: []uint64{
				chanID2.ToUint64(),
			},
			amount:         20000,
			maxParts:       5,
			err:            errBalanceTooLow,
			expectedTarget: 0,
		},
		{
			name:       "amount with routing fee too high",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2,
			},
			amount:         10000,
			maxRoutingFee:  100,
			maxParts:       5,
			err:            errBalanceTooLow,
			expectedTarget: 0,
		},
		{
			name:       "can split between channels",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2, channel4,
			},
			amount:         11000,
			maxParts:       16,
			err:            nil,
			expectedTarget: 2,
		},
		{
			name:       "can't split between channels",
			chain:      chaincfg.MainNetParams,
			destAddr:   mainnetAddr,
			label:      "label ok",
			confTarget: 2,
			channels: []lndclient.ChannelInfo{
				channel2, channel4,
			},
			amount:         11000,
			maxParts:       5,
			err:            errBalanceTooLow,
			expectedTarget: 0,
		},
		{
			name:           "node pubkey as dest addr",
			chain:          chaincfg.MainNetParams,
			destAddr:       nodepubkeyAddr,
			err:            errInvalidAddress,
			expectedTarget: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			lnd := mock_lnd.NewMockLnd()
			lnd.Channels = test.channels

			req := &looprpc.LoopOutRequest{
				Amt:               test.amount,
				MaxSwapRoutingFee: test.maxRoutingFee,
				OutgoingChanSet:   test.outgoingChanSet,
				Label:             test.label,
				SweepConfTarget:   test.confTarget,
			}

			logger := btclog.NewSLogger(
				btclog.NewDefaultHandler(os.Stdout),
			)
			setLogger(logger.SubSystem(Subsystem))

			conf, err := validateLoopOutRequest(
				ctx, lnd.Client, &test.chain, req,
				test.destAddr, test.maxParts,
			)
			require.ErrorIs(t, err, test.err)
			require.Equal(t, test.expectedTarget, conf)
		})
	}
}

// TestHasBandwidth tests that the hasBandwidth function correctly simulates
// the MPP logic used by LND.
func TestHasBandwidth(t *testing.T) {
	tests := []struct {
		name           string
		channels       []lndclient.ChannelInfo
		maxParts       int
		amt            btcutil.Amount
		expectedRes    bool
		expectedShards int
	}{
		{
			name: "can route due to high number of parts",
			channels: []lndclient.ChannelInfo{
				{
					LocalBalance: 100,
				},
				{
					LocalBalance: 10,
				},
			},
			maxParts:       11,
			amt:            110,
			expectedRes:    true,
			expectedShards: 8,
		},
		{
			name: "can't route due to low number of parts",
			channels: []lndclient.ChannelInfo{
				{
					LocalBalance: 100,
				},
				{
					LocalBalance: 10,
				},
			},
			maxParts:    5,
			amt:         110,
			expectedRes: false,
		},
		{
			name: "can route",
			channels: []lndclient.ChannelInfo{
				{
					LocalBalance: 1000,
				},
				{
					LocalBalance: 1000,
				},
			},
			maxParts:       5,
			amt:            2000,
			expectedRes:    true,
			expectedShards: 2,
		},
		{
			name: "can route",
			channels: []lndclient.ChannelInfo{
				{
					LocalBalance: 100,
				},
				{
					LocalBalance: 100,
				},
				{
					LocalBalance: 100,
				},
			},
			maxParts:       10,
			amt:            300,
			expectedRes:    true,
			expectedShards: 10,
		},
		{
			name:           "can't route due to empty channel set",
			maxParts:       10,
			amt:            300,
			expectedRes:    false,
			expectedShards: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			res, shards := hasBandwidth(test.channels, test.amt,
				test.maxParts)
			require.Equal(t, test.expectedRes, res)
			require.Equal(t, test.expectedShards, shards)
		})
	}
}

// TestListSwapsFilterAndPagination tests the filtering and
// paging of the ListSwaps command.
func TestListSwapsFilterAndPagination(t *testing.T) {
	unixTime := time.Unix(0, 0)
	firstSwapStartTime := unixTime.Add(10 * time.Minute)
	secondSwapStartTime := unixTime.Add(20 * time.Minute)
	thirdSwapStartTime := unixTime.Add(30 * time.Minute)

	// Create a set of test swaps of various types which contain the minimal
	// viable amount of info to successfully be run through marshallSwap.
	swapInOrder0 := loop.SwapInfo{
		SwapStateData: loopdb.SwapStateData{
			State: loopdb.StateInitiated,
			Cost:  loopdb.SwapCost{},
		},
		SwapContract: loopdb.SwapContract{
			InitiationTime: firstSwapStartTime,
		},
		LastUpdate:       time.Now(),
		SwapHash:         lntypes.Hash{1},
		SwapType:         swap.TypeIn,
		HtlcAddressP2WSH: testnetAddr,
		HtlcAddressP2TR:  testnetAddr,
	}

	swapOutOrder1 := loop.SwapInfo{
		SwapStateData: loopdb.SwapStateData{
			State: loopdb.StateInitiated,
			Cost:  loopdb.SwapCost{},
		},
		SwapContract: loopdb.SwapContract{
			InitiationTime: secondSwapStartTime,
		},
		LastUpdate:       time.Now(),
		SwapHash:         lntypes.Hash{2},
		SwapType:         swap.TypeOut,
		HtlcAddressP2WSH: testnetAddr,
		HtlcAddressP2TR:  testnetAddr,
	}

	swapOutOrder2 := loop.SwapInfo{
		SwapStateData: loopdb.SwapStateData{
			State: loopdb.StateInitiated,
			Cost:  loopdb.SwapCost{},
		},
		SwapContract: loopdb.SwapContract{
			InitiationTime: thirdSwapStartTime,
		},
		LastUpdate:       time.Now(),
		SwapHash:         lntypes.Hash{3},
		SwapType:         swap.TypeOut,
		HtlcAddressP2WSH: testnetAddr,
		HtlcAddressP2TR:  testnetAddr,
	}

	mockSwaps := []loop.SwapInfo{swapInOrder0, swapOutOrder1, swapOutOrder2}

	tests := []struct {
		name string
		// Define the mock swaps that will be stored in the mock client.
		mockSwaps []loop.SwapInfo
		req       *looprpc.ListSwapsRequest
		// These hashes must be in the correct return order as the response.
		expectedReturnedSwaps []lntypes.Hash
		expectedNextStartTime int64
	}{
		{
			name:      "fetch with defaults",
			mockSwaps: mockSwaps,
			req:       &looprpc.ListSwapsRequest{},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with swaptype=loopin filter",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					SwapType: looprpc.ListSwapsFilter_LOOP_IN},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with swaptype=loopout filter",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					SwapType: looprpc.ListSwapsFilter_LOOP_OUT,
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with limit",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				MaxSwaps: 2,
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
			},
			expectedNextStartTime: secondSwapStartTime.UnixNano() + 1,
		},
		{
			name:      "fetch with limit set to default",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				MaxSwaps: 0,
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with time filter #1",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: unixTime.Add(25 * time.Minute).UnixNano(),
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with time filter #2",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: unixTime.Add(5 * time.Minute).UnixNano(),
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with swaptype=loopout filter, time filter, limit set",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					SwapType:         looprpc.ListSwapsFilter_LOOP_OUT,
					StartTimestampNs: unixTime.Add(15 * time.Minute).UnixNano(),
				},
				MaxSwaps: 1,
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder1.SwapHash,
			},
			expectedNextStartTime: secondSwapStartTime.UnixNano() + 1,
		},
		{
			name:      "fetch with time filter, limit set",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: unixTime.UnixNano(),
				},
				MaxSwaps: 2,
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
			},
			expectedNextStartTime: secondSwapStartTime.UnixNano() + 1,
		},
		{
			name:      "fetch with time filter, limit set 2",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: unixTime.UnixNano(),
				},
				MaxSwaps: 3,
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapInOrder0.SwapHash,
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with time filter edge case 1",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: secondSwapStartTime.UnixNano(),
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with time filter edge case 2",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: secondSwapStartTime.UnixNano() + 1,
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
		{
			name:      "fetch with time filter edge case 3",
			mockSwaps: mockSwaps,
			req: &looprpc.ListSwapsRequest{
				ListSwapFilter: &looprpc.ListSwapsFilter{
					StartTimestampNs: secondSwapStartTime.UnixNano() - 1,
				},
			},
			expectedReturnedSwaps: []lntypes.Hash{
				swapOutOrder1.SwapHash,
				swapOutOrder2.SwapHash,
			},
			expectedNextStartTime: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Create the swap client server with our mock client.
			server := &swapClientServer{
				swaps: make(map[lntypes.Hash]loop.SwapInfo),
			}

			// Populate the server's swap cache with our mock swaps.
			for _, swap := range test.mockSwaps {
				server.swaps[swap.SwapHash] = swap
			}

			// Call the ListSwaps method.
			resp, err := server.ListSwaps(context.Background(), test.req)
			require.NoError(t, err)

			require.Len(
				t,
				resp.Swaps,
				len(test.expectedReturnedSwaps),
				"incorrect returned count",
			)

			// Check order of returned swaps is exactly as expected.
			for idx, aswap := range resp.Swaps {
				newhash, err := lntypes.MakeHash(aswap.GetIdBytes())
				require.NoError(t, err)
				require.Equal(
					t,
					test.expectedReturnedSwaps[idx],
					newhash,
					"iteration order mismatch",
				)
			}

			require.Equal(
				t,
				test.expectedNextStartTime,
				resp.NextStartTime,
				"incorrect next start time",
			)
		})
	}
}
