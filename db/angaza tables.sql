-- MySQL Setup Database

-- -----------------------------------------------------
-- Schema Angaza
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS Angaza ;
USE Angaza ;

/* Drop tables */
DROP TABLE IF EXISTS Angaza.payments;
DROP TABLE IF EXISTS Angaza.accounts;
DROP TABLE IF EXISTS Angaza.clients;

-- -----------------------------------------------------
-- Table Angaza.clients
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Angaza.clients (
  client_id                                 INT             NOT NULL AUTO_INCREMENT,
  /* client_abbv                               VARCHAR(2)      NOT NULL DEFAULT 'CL', */
  client_external_id                        VARCHAR(45)     NULL UNIQUE,
  organization                              VARCHAR(45)     NOT NULL,
  country                                   VARCHAR(45)     NOT NULL,
  client_name                               VARCHAR(45)     NOT NULL, 
  phone_number                              INT             NOT NULL,
  account_numbers                           VARCHAR(250)    NOT NULL,
  recorder                                  VARCHAR(100)    NOT NULL,
  date_created_utc                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  archived                                  BOOLEAN         DEFAULT 0,
  
  /* Fields linked to  accounts/credit questions */
  client_photo                              VARCHAR(250)    NULL,

  -- System fields
  zoho_id                                   BIGINT          NULL,
  added_user                                VARCHAR(45)     NOT NULL,
  added_timestamp                           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_timestamp                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  bus_effective_from                        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  bus_effective_to                          DATETIME        NOT NULL DEFAULT '9999-12-31 23:59:59',

  PRIMARY KEY (client_id),
    index(client_name),
    index(client_id, client_external_id)
  );


-- -----------------------------------------------------
-- Table Angaza.accounts
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Angaza.accounts (
  -- Angaza fields
  account_id                                INT             NOT NULL AUTO_INCREMENT,
  /* account_abbv                              VARCHAR(2)      NOT NULL DEFAULT 'AC', */
  account_external_id                       VARCHAR(45)     NULL UNIQUE,

  account_number                            INT             NULL,
  previous_account_number                   INT             NULL,
  date_of_repossession_utc                  TIMESTAMP       NULL,
  group_name                                VARCHAR(100)    NULL,
  product_name                              VARCHAR(100)    NULL,
  organization                              VARCHAR(100)    NULL,
  country                                   VARCHAR(100)    NULL,

  account_status                            VARCHAR(45)     NULL,
  registration_date_utc                     TIMESTAMP       NULL,
  registering_user                          VARCHAR(255)    NULL,
  registering_user_angaza_id                VARCHAR(45)     NULL,
  registration_location_latitudelongitude   VARCHAR(255)    NULL,

  upfront_price                             DECIMAL (18,2)  NULL,
  upfront_days_included                     DECIMAL (18,2)  NULL,
  unlock_price                              DECIMAL (18,2)  NULL,
  minimum_payment_amount                    DECIMAL (18,2)  NULL,

  number_of_payments                        INT             NULL,
  days_to_cutoff                            DECIMAL (18,2)  NULL,
  expected_paid                             DECIMAL (18,2)  NULL,
  total_paid                                DECIMAL (18,2)  NULL,
  cumulative_days_disabled                  DECIMAL (18,2)  NULL,
  
  date_of_latest_payment_utc                TIMESTAMP       NULL,
  date_of_disablement_utc                   TIMESTAMP       NULL,
  date_of_write_off                         TIMESTAMP       NULL,

  -- Customer info
  client_id                                 INT             NOT NULL,
  client_external_id                        VARCHAR(45)     NULL,
  owner_msisdn                              VARCHAR(45)     NULL,
  owner_name                                VARCHAR(255)    NULL,
  owner_location                            VARCHAR(100)    NULL,
  next_of_kin_contact_number                VARCHAR(45)     NULL,
  responsible_user                          VARCHAR(255)    NULL,
  responsible_user_angaza_id                VARCHAR(45)     NULL,
  responsible_user_since                    TIMESTAMP       NULL,
  
  -- customer_gender,
  -- customer_age,
  -- customer_region,
  -- customer_occupation,
  -- customer_affiliation,
  -- customer_id,

  -- Future fields - NEWFEEFEE and more
  intial_call_status                        VARCHAR(45)     NULL,
  initial_call_timestamp                    TIMESTAMP       NULL,

  -- System fields
  zoho_id                                   BIGINT          NULL,
  added_user                                VARCHAR(45)     NULL,
  added_timestamp                           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_timestamp                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (account_id),
  INDEX account_number (account_number),
  INDEX zoho_id (zoho_id)

  -- CONSTRAINT fk_client_id FOREIGN KEY (client_id)
  --   REFERENCES clients(client_id)
);

-- -----------------------------------------------------
-- Table Angaza.payments
-- -----------------------------------------------------
/* CREATE TABLE Angaza.payments (
  
  payment_id                                INT             NOT NULL AUTO_INCREMENT,
  payment_external_id                       VARCHAR(45)     NULL,  
  organization                              VARCHAR(100)    NULL,
  country                                   VARCHAR(100)    NULL,
  effective_utc                             TIMESTAMP       NOT NULL,
  account_number                            INT             NOT NULL,
  account_id                                INT             NOT NULL,
  amount                                    DECIMAL(15,2)   NOT NULL,

  -- System fields
  zoho_id                                   BIGINT          NULL,
  added_user                                VARCHAR(45)     NULL,
  added_timestamp                           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_timestamp                          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (payment_id)

  -- CONSTRAINT fk_account_id FOREIGN KEY (client_id)
  --   REFERENCES clients(client_id)
); */



-- -----------------------------------------------------
-- Table Angaza.ZohoAPIadd
-- -----------------------------------------------------
-- DROP TABLE IF EXISTS Angaza.zohoExecution;
-- CREATE TABLE IF NOT EXISTS Angaza.clients (
--   zohoID BIGINT NOT NULL,
--   client_abbv VARCHAR(2) NOT NULL DEFAULT 'CL',
--   client_angaza_id VARCHAR(45) NOT NULL DEFAULT (CONCAT(client_abbv,client_id)),
--   organization VARCHAR(45) NOT NULL,
--   client_name VARCHAR(45) NOT NULL, 
--   phone_number BIGINT NOT NULL,
--   account_numbers VARCHAR(45) NOT NULL,
--   recorder VARCHAR(45) NOT NULL,
--   date_created_utc TIMESTAMP NOT NULL,
--   archived BOOLEAN,
--   PRIMARY KEY (client_id)
--   );