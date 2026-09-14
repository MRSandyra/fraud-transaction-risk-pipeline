CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    external_id     VARCHAR(64) UNIQUE NOT NULL,   
    name            VARCHAR(128) NOT NULL,
    account_balance NUMERIC(14,2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS merchants (
    id              SERIAL PRIMARY KEY,
    external_id     VARCHAR(64) UNIQUE NOT NULL,   
    name            VARCHAR(128) NOT NULL,
    category        VARCHAR(64) NOT NULL,
    risk_score      NUMERIC(3,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL PRIMARY KEY,
    step            INTEGER NOT NULL,             
    type            VARCHAR(32) NOT NULL,           
    amount          NUMERIC(14,2) NOT NULL,
    name_orig       VARCHAR(64) NOT NULL,
    name_dest       VARCHAR(64) NOT NULL,
    is_fraud        BOOLEAN NOT NULL DEFAULT false,
    inserted_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_step ON transactions (step);
CREATE INDEX IF NOT EXISTS idx_transactions_orig ON transactions (name_orig);
