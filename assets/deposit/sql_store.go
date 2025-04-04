package deposit

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/loop/loopdb"
	"github.com/lightninglabs/loop/loopdb/sqlc"
	"github.com/lightninglabs/taproot-assets/address"
	"github.com/lightninglabs/taproot-assets/asset"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/keychain"
)

// Querier is a subset of the methods we need from the postgres.querier
// interface for the deposit store.
type Querier interface {
	AddAssetDeposit(context.Context, sqlc.AddAssetDepositParams) error

	GetAssetDeposit(ctx context.Context, depositID string) (
		sqlc.GetAssetDepositRow, error)

	GetAssetDeposits(ctx context.Context) ([]sqlc.GetAssetDepositsRow,
		error)

	GetActiveAssetDeposits(ctx context.Context) (
		[]sqlc.GetActiveAssetDepositsRow, error)

	MarkDepositConfirmed(ctx context.Context,
		arg sqlc.MarkDepositConfirmedParams) error

	UpdateDepositState(ctx context.Context,
		arg sqlc.UpdateDepositStateParams) error

	GetAssetDepositLeasedUTXOs(ctx context.Context) (
		[]sqlc.GetAssetDepositLeasedUTXOsRow, error)

	AssetDeposiLeaseUTXO(ctx context.Context,
		arg sqlc.AssetDeposiLeaseUTXOParams) error

	AssetDepositReleaseUTXOs(ctx context.Context,
		depositID string) error
}

// DepositBaseDB is the interface that contains all the queries generated
// by sqlc for the deposit store. It also includes the ExecTx method for
// executing a function in the context of a database transaction.
type DepositBaseDB interface {
	Querier

	// ExecTx allows for executing a function in the context of a database
	// transaction.
	ExecTx(ctx context.Context, txOptions loopdb.TxOptions,
		txBody func(Querier) error) error
}

// SQLStore is the high level SQL store for deposits.
type SQLStore struct {
	db DepositBaseDB

	clock clock.Clock
}

// NewSQLStore creates a new SQLStore.
func NewSQLStore(db DepositBaseDB, clock clock.Clock) *SQLStore {
	return &SQLStore{
		db:    db,
		clock: clock,
	}
}

// AddAssetDeposit adds a new asset deposit to the database.
func (s *SQLStore) AddAssetDeposit(ctx context.Context, d *Deposit) error {
	txOptions := loopdb.NewSqlWriteOpts()

	createdAt := d.CreatedAt.UTC()
	return s.db.ExecTx(ctx, txOptions, func(tx Querier) error {
		err := tx.AddAssetDeposit(ctx, sqlc.AddAssetDepositParams{
			DepositID:       d.ID,
			CreatedAt:       createdAt,
			AssetID:         d.AssetID[:],
			Amount:          int64(d.Amount),
			ClientPubkey:    d.FunderKey.SerializeCompressed(),
			ServerPubkey:    d.CoSignerKey.SerializeCompressed(),
			Expiry:          int32(d.CsvExpiry),
			ClientKeyFamily: int32(d.KeyLocator.Family),
			ClientKeyIndex:  int32(d.KeyLocator.Index),
			Addr:            d.Addr,
			ProtocolVersion: int32(d.ProtocolVersion),
		})
		if err != nil {
			return err
		}

		return tx.UpdateDepositState(ctx, sqlc.UpdateDepositStateParams{
			DepositID:       d.ID,
			UpdateState:     int32(StateInitiated),
			UpdateTimestamp: createdAt,
		})
	})
}

func (s *SQLStore) AddDepositUpdate(ctx context.Context,
	depositID string, state State) error {

	return s.db.UpdateDepositState(
		ctx, sqlc.UpdateDepositStateParams{
			DepositID:       depositID,
			UpdateState:     int32(state),
			UpdateTimestamp: s.clock.Now().UTC(),
		},
	)
}

func (s *SQLStore) MarkDepositConfirmed(ctx context.Context,
	depositID string, outpoint string, pkScript []byte,
	confirmationHeight uint32) error {

	txOptions := loopdb.NewSqlWriteOpts()

	return s.db.ExecTx(ctx, txOptions, func(tx Querier) error {
		err := tx.MarkDepositConfirmed(
			ctx, sqlc.MarkDepositConfirmedParams{
				DepositID: depositID,
				ConfirmationHeight: sql.NullInt32{
					Int32: int32(confirmationHeight),
					Valid: true,
				},
				Outpoint: sql.NullString{
					String: outpoint,
					Valid:  true,
				},
				PkScript: pkScript,
			},
		)
		if err != nil {
			return err
		}

		return tx.UpdateDepositState(ctx, sqlc.UpdateDepositStateParams{
			DepositID:       depositID,
			UpdateState:     int32(StateConfirmed),
			UpdateTimestamp: s.clock.Now().UTC(),
		})
	})
}

func (s *SQLStore) UpdateDepositState(ctx context.Context,
	depositID string, state State) error {

	return s.db.UpdateDepositState(
		ctx, sqlc.UpdateDepositStateParams{
			DepositID:       depositID,
			UpdateState:     int32(state),
			UpdateTimestamp: s.clock.Now().UTC(),
		},
	)
}

func (s *SQLStore) GetDeposit(ctx context.Context, depositID string) (
	Deposit, error) {

	sqlDeposit, err := s.db.GetAssetDeposit(ctx, depositID)
	if err != nil {
		return Deposit{}, err
	}

	return sqlcDepositToDeposit(sqlc.GetAssetDepositsRow(sqlDeposit))
}

func (s *SQLStore) GetAllDeposits(ctx context.Context) ([]Deposit, error) {
	sqlDeposits, err := s.db.GetAssetDeposits(ctx)
	if err != nil {
		return nil, err
	}

	deposits := make([]Deposit, 0, len(sqlDeposits))
	for _, sqlDeposit := range sqlDeposits {
		deposit, err := sqlcDepositToDeposit(sqlDeposit)
		if err != nil {
			return nil, err
		}

		deposits = append(deposits, deposit)
	}

	return deposits, nil
}

func (s *SQLStore) GetActiveDeposits(ctx context.Context) ([]Deposit, error) {
	sqlDeposits, err := s.db.GetActiveAssetDeposits(ctx)
	if err != nil {
		return nil, err
	}

	deposits := make([]Deposit, 0, len(sqlDeposits))
	for _, sqlDeposit := range sqlDeposits {
		deposit, err := sqlcDepositToDeposit(
			sqlc.GetAssetDepositsRow(sqlDeposit),
		)
		if err != nil {
			return nil, err
		}

		deposits = append(deposits, deposit)
	}

	return deposits, nil
}

func (s *SQLStore) GetLeasedUTXOs(ctx context.Context) (
	map[string][]wire.OutPoint, error) {

	rows, err := s.db.GetAssetDepositLeasedUTXOs(ctx)
	if err != nil {
		return nil, err
	}

	leasedUTXOs := make(map[string][]wire.OutPoint)
	for _, row := range rows {
		outpoint, err := wire.NewOutPointFromString(row.Outpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse outpoint: %w",
				err)
		}

		leasedUTXOs[row.DepositID] = append(
			leasedUTXOs[row.DepositID], *outpoint,
		)
	}

	return leasedUTXOs, nil
}

func (s *SQLStore) AssetDepositLeaseUTXO(ctx context.Context, depositID string,
	outpoint wire.OutPoint) error {

	return s.db.AssetDeposiLeaseUTXO(ctx,
		sqlc.AssetDeposiLeaseUTXOParams{
			DepositID: depositID,
			Outpoint:  outpoint.String(),
		},
	)
}

func (s *SQLStore) AssetDepositReleaseUTXOs(ctx context.Context,
	depositID string) error {

	return s.db.AssetDepositReleaseUTXOs(ctx, depositID)
}

func sqlcDepositToDeposit(sqlDeposit sqlc.GetAssetDepositsRow) (Deposit, error) {
	clientPubKey, err := btcec.ParsePubKey(sqlDeposit.ClientPubkey)
	if err != nil {
		return Deposit{}, err
	}

	serverPubKey, err := btcec.ParsePubKey(sqlDeposit.ServerPubkey)
	if err != nil {
		return Deposit{}, err
	}

	clientKeyDesc := keychain.KeyDescriptor{
		PubKey: clientPubKey,
		KeyLocator: keychain.KeyLocator{
			Family: keychain.KeyFamily(
				sqlDeposit.ClientKeyFamily,
			),
			Index: uint32(sqlDeposit.ClientKeyIndex),
		},
	}

	if len(sqlDeposit.AssetID) != len(asset.ID{}) {
		return Deposit{}, fmt.Errorf("malformed asset ID for deposit: "+
			"%v", sqlDeposit.DepositID)
	}

	// TODO(bhandras): chainparams!
	var addressParams *address.ChainParams
	kit, err := NewKit(
		clientKeyDesc.PubKey, serverPubKey, clientKeyDesc.KeyLocator,
		asset.ID(sqlDeposit.AssetID), uint32(sqlDeposit.Expiry),
		addressParams,
	)
	if err != nil {
		return Deposit{}, err
	}

	deposit := Deposit{
		Kit: kit,
		ID:  sqlDeposit.DepositID,
		ProtocolVersion: AssetDepositProtocolVersion(
			sqlDeposit.ProtocolVersion,
		),
		CreatedAt: sqlDeposit.CreatedAt.Local(),
		Amount:    uint64(sqlDeposit.Amount),
		Addr:      sqlDeposit.Addr,
		State:     State(sqlDeposit.UpdateState),
		PkScript:  sqlDeposit.PkScript,
	}

	if sqlDeposit.ConfirmationHeight.Valid {
		deposit.ConfirmationHeight = uint32(
			sqlDeposit.ConfirmationHeight.Int32,
		)
	}

	if sqlDeposit.Outpoint.Valid {
		deposit.Outpoint = sqlDeposit.Outpoint.String
	}

	return deposit, nil
}
