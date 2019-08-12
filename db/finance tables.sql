-- MySQL Setup Database

-- -----------------------------------------------------
-- Schema
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS Finance ;
USE Finance ;

/* Drop tables */
DROP TABLE IF EXISTS Finance.cashflow;
DROP TABLE IF EXISTS Finance.mobile;

-- -----------------------------------------------------
-- Table Finance.cashflow;
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Finance.cashflow (
  transaction_id                            INT             NOT NULL AUTO_INCREMENT,
  transaction_date                          TIMESTAMP       NOT NULL,
  account_paid_from                         VARCHAR(100)    NOT NULL,
  account_paid_into                         VARCHAR(100)    NOT NULL,
  cashflow_category                         VARCHAR(100)    NOT NULL,
  currency                                  VARCHAR(3)      NOT NULL,
  amount                                    DECIMAL(15,2)   NOT NULL,
  description                               VARCHAR(120)    NOT NULL,
  comments                                  VARCHAR(1200)   NOT NULL,

  -- System fields
  zoho_id                                   BIGINT          NULL,
  added_user                                VARCHAR(45)     NOT NULL,
  added_timestamp                           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_timestamp                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (transaction_id)
  );


-- -----------------------------------------------------
-- Table Finance.mobile;
-- -----------------------------------------------------
/* CREATE TABLE IF NOT EXISTS Finance.mobile (
  trn_id                                    INT             NOT NULL AUTO_INCREMENT,
  provider_id                               VARCHAR(100)    NOT NULL,
  ipns_id                                   VARCHAR(100)    NOT NULL,
  trn_timestamp                             TIMESTAMP       NOT NULL,
  trn_ref_number                            VARCHAR(100)    NOT NULL,
  currency                                  VARCHAR(3)      NOT NULL,
  trn_amount                                DECIMAL(15,2)   NOT NULL DEFAULT 0,
  trn_type                                  VARCHAR(100)    NOT NULL,
  service_type                              VARCHAR(100)    NOT NULL,
  sender_number                             INT             NOT NULL,
  receiver_number                           INT             NOT NULL,
  
  prev_acc_bal                              DECIMAL(15,2)   NOT NULL,
  post_acc_bal                              DECIMAL(15,2)   NOT NULL,
  note                                      VARCHAR(1200)   NOT NULL,
  
  -- System fields
  added_user                                VARCHAR(45)     NOT NULL,
  added_timestamp                           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_timestamp                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (trn_id),
    index(provider_id)
  ); */
