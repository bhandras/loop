-- name: AddAssetDeposit :exec
INSERT INTO asset_deposits (
    deposit_id,
    created_at,
    asset_id,
    amount,
    client_pubkey,
    server_pubkey,
    expiry,
    client_key_family,
    client_key_index,
    addr,
    protocol_version
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: MarkDepositConfirmed :exec
UPDATE asset_deposits SET confirmation_height = $2 WHERE deposit_id = $1;

-- name: UpdateDepositState :exec
INSERT INTO asset_deposit_updates (
    deposit_id,
    update_state,
    update_timestamp
) VALUES ($1, $2, $3);

-- name: GetAssetDeposit :one
SELECT d.*, u.update_state, u.update_timestamp
FROM asset_deposits d
JOIN asset_deposit_updates u ON u.id = (
    SELECT id
    FROM asset_deposit_updates
    WHERE deposit_id = d.deposit_id
    ORDER BY update_timestamp DESC
    LIMIT 1
)
WHERE d.deposit_id = $1;

-- name: GetAssetDeposits :many
SELECT d.*, u.update_state, u.update_timestamp
FROM asset_deposits d
JOIN asset_deposit_updates u ON u.id = (
    SELECT id
    FROM asset_deposit_updates
    WHERE deposit_id = d.deposit_id
    ORDER BY update_timestamp DESC
    LIMIT 1
)
ORDER BY d.created_at ASC;

-- name: GetActiveAssetDeposits :many
SELECT d.*, u.update_state, u.update_timestamp
FROM asset_deposits d
JOIN asset_deposit_updates u
  ON u.deposit_id = d.deposit_id
WHERE u.id = (
    SELECT id
    FROM asset_deposit_updates
    WHERE deposit_id = d.deposit_id
    ORDER BY update_timestamp DESC
    LIMIT 1
)
AND u.update_state IN (0, 1, 2);
