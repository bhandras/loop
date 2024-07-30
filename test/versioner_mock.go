package test

import (
	"context"
	"time"

	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/lnrpc/verrpc"
)

const (
	defaultMockCommit        = "v0.99.9-beta"
	defaultMockCommitHash    = "0000000000000000000000000000000000000000"
	defaultMockVersion       = "v0.99.9-beta"
	defaultMockAppMajor      = 0
	defaultMockAppMinor      = 99
	defaultMockAppPatch      = 9
	defaultMockAppPrerelease = "beta"
	defaultMockAppGoVersion  = "go1.99.9"
)

var (
	defaultMockBuildTags = []string{
		"signrpc", "walletrpc", "chainrpc", "invoicesrpc",
	}
)

type mockVersioner struct {
	version *verrpc.Version
}

var _ lndclient.VersionerClient = (*mockVersioner)(nil)

func newMockVersioner() *mockVersioner {
	return &mockVersioner{
		version: &verrpc.Version{
			Commit:        defaultMockCommit,
			CommitHash:    defaultMockCommitHash,
			Version:       defaultMockVersion,
			AppMajor:      defaultMockAppMajor,
			AppMinor:      defaultMockAppMinor,
			AppPatch:      defaultMockAppPatch,
			AppPreRelease: defaultMockAppPrerelease,
			BuildTags:     defaultMockBuildTags,
			GoVersion:     defaultMockAppGoVersion,
		},
	}
}

func (v *mockVersioner) GetVersion(_ context.Context) (*verrpc.Version, error) {
	return v.version, nil
}

// RawClientWithMacAuth returns a context with the proper macaroon
// authentication, the default RPC timeout, and the raw client. Note that this
// is only included for compatibility with the interface and does not actually
// return a client.
func (v *mockVersioner) RawClientWithMacAuth(
	parentCtx context.Context) (context.Context, time.Duration,
	verrpc.VersionerClient) {

	return parentCtx, defaultRpcTimeout, nil
}
