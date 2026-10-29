-- Coral Digital Bank: core banking source tables (SQL Server / Azure SQL Database)
-- Run once against your source database. load_sqlserver.py --init runs this for you.
-- All timestamps are UTC.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core')
    EXEC('CREATE SCHEMA core');
GO

DROP TABLE IF EXISTS core.transactions;
DROP TABLE IF EXISTS core.accounts;
DROP TABLE IF EXISTS core.customers;
DROP TABLE IF EXISTS core.branches;
GO

CREATE TABLE core.branches (
    branch_id      VARCHAR(10)   NOT NULL PRIMARY KEY,
    bsb            CHAR(7)       NOT NULL,
    branch_name    VARCHAR(80)   NOT NULL,
    city           VARCHAR(50)   NOT NULL,
    state          VARCHAR(3)    NOT NULL,
    region         VARCHAR(10)   NOT NULL,      -- METRO / REGIONAL
    opened_date    DATE          NOT NULL,
    updated_at     DATETIME2(0)  NOT NULL
);

CREATE TABLE core.customers (
    customer_id     VARCHAR(10)   NOT NULL PRIMARY KEY,
    first_name      VARCHAR(50)   NOT NULL,
    last_name       VARCHAR(50)   NOT NULL,
    date_of_birth   DATE          NOT NULL,
    email           VARCHAR(120)  NOT NULL,
    mobile          VARCHAR(20)   NOT NULL,
    state           VARCHAR(3)    NOT NULL,
    postcode        CHAR(4)       NOT NULL,
    home_branch_id  VARCHAR(10)   NOT NULL REFERENCES core.branches(branch_id),
    kyc_status      VARCHAR(20)   NOT NULL,     -- VERIFIED / PENDING / UNDER_REVIEW
    customer_since  DATE          NOT NULL,
    updated_at      DATETIME2(0)  NOT NULL      -- incremental watermark
);
CREATE INDEX ix_customers_updated_at ON core.customers(updated_at);

CREATE TABLE core.accounts (
    account_id       VARCHAR(10)    NOT NULL PRIMARY KEY,
    customer_id      VARCHAR(10)    NOT NULL REFERENCES core.customers(customer_id),
    product_type     VARCHAR(20)    NOT NULL,   -- EVERYDAY / SAVINGS / TERM_DEPOSIT / CREDIT_CARD / PERSONAL_LOAN / HOME_LOAN
    bsb              CHAR(7)        NOT NULL,
    account_status   VARCHAR(10)    NOT NULL,
    opened_date      DATE           NOT NULL,
    interest_rate    DECIMAL(6,4)   NOT NULL,
    credit_limit     DECIMAL(12,2)  NULL,
    current_balance  DECIMAL(14,2)  NOT NULL,   -- negative for loans and card debt
    updated_at       DATETIME2(0)   NOT NULL    -- incremental watermark
);
CREATE INDEX ix_accounts_updated_at ON core.accounts(updated_at);

CREATE TABLE core.transactions (
    transaction_id     VARCHAR(12)    NOT NULL PRIMARY KEY,
    account_id         VARCHAR(10)    NOT NULL REFERENCES core.accounts(account_id),
    txn_timestamp      DATETIME2(0)   NOT NULL, -- incremental watermark (append-only)
    direction          VARCHAR(6)     NOT NULL, -- DEBIT / CREDIT
    channel            VARCHAR(20)    NOT NULL, -- CARD / NPP / BPAY / ATM / DIRECT_DEBIT / BRANCH / INTERNAL / INTERNAL_TRANSFER
    amount             DECIMAL(14,2)  NOT NULL, -- always positive; direction gives the sign
    merchant_category  VARCHAR(20)    NULL,
    description        VARCHAR(100)   NOT NULL,
    txn_status         VARCHAR(10)    NOT NULL  -- POSTED / DECLINED / REVERSED
);
CREATE INDEX ix_transactions_txn_timestamp ON core.transactions(txn_timestamp);
GO
