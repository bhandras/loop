CREATE TABLE IF NOT EXISTS asset_deposits (
        deposit_id TEXT PRIMARY KEY,

        -- protocol_version is the protocol version that the deposit was
	-- created with.
	protocol_version INTEGER NOT NULL,

        -- created_at is the time at which the deposit was created.
        created_at TIMESTAMP NOT NULL,

        -- asset_id is the asset that is being deposited.
        asset_id BYTEA NOT NULL,

	-- amount is the amount of the deposit in asset units.
        amount BIGINT NOT NULL,

	-- client_pubkey is the client side public key that is used to
	-- construct the 2-of-2 MuSig2 anchor output that holds the deposited
	-- funds.
        client_pubkey BYTEA NOT NULL,

	-- server_pubkey is the server side public key that is used to
	-- construct the 2-of-2 MuSig2 anchor output that holds the deposited
	-- funds.
        server_pubkey BYTEA NOT NULL,

        -- expiry denotes the CSV delay at which funds at a specific static
        -- address can be swept back to the client.
        expiry INT NOT NULL,

        -- client_key_family is the key family of the client public key from the
        -- client's lnd wallet.
        client_key_family INT NOT NULL,

        -- client_key_index is the key index of the client public key from the
        -- client's lnd wallet.
        client_key_index INT NOT NULL,

	-- addr is the TAP deposit address that the client should send the funds
	-- to.
	addr STRING NOT NULL UNIQUE,

        -- confirmation_height is the block height at which the deposit was
        -- confirmed on-chain.
        confirmation_height INT,

        -- outpoint is the outpoint of the confirmed deposit.
        outpoint STRING,

        -- pk_script is the pkScript of the deposit output.
        pk_script BYTEA
);

-- asset_deposit_updates contains all the updates to an asset deposit.
CREATE TABLE IF NOT EXISTS asset_deposit_updates (
    -- id is the auto incrementing primary key.
    id INTEGER PRIMARY KEY,

    -- deposit_id is the unique identifier for the deposit.
    deposit_id TEXT NOT NULL REFERENCES asset_deposits(deposit_id),

    -- update_state is the state of the deposit at the time of the update.
    update_state INT NOT NULL,

    -- update_timestamp is the timestamp of the update.
    update_timestamp TIMESTAMP NOT NULL
);

-- asset_deposit_leased_utxos contains all the UTXOs that were leased to a
-- particular deposit. These leased UTXOs are used to fund the deposit timeout
-- sweep transaction.
CREATE TABLE IF NOT EXISTS asset_deposit_leased_utxos (
    -- id is the auto incrementing primary key.
    id INTEGER PRIMARY KEY,

    -- deposit_id is the unique identifier for the deposit.
    deposit_id TEXT NOT NULL REFERENCES asset_deposits(deposit_id),

    -- outpoint is the outpoint of the UTXO that was leased.
    outpoint STRING NOT NULL
);
