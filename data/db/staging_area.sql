BEGIN TRANSACTION;
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS "audit_events" (
	"last_event" DATETIME NULL);

-- stage tables
CREATE TABLE IF NOT EXISTS "customer_logins_stg" (
	"timestamp"	DATETIME NULL,
	"site"	TEXT NULL,
	"customer_id"	TEXT NULL);

CREATE TABLE IF NOT EXISTS "customer_registration_stg" (
	"timestamp" DATETIME NULL,
	"site" TEXT NULL,
	"customeremail" TEXT NULL,
	"dateofbirth" DATETIME NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" DATETIME NULL,
	"customernumber" TEXT NULL);

CREATE TABLE IF NOT EXISTS "games_purchase_stg" (
	"timestamp"	DATETIME NULL,
	"sitetid"	TEXT NULL,
	"customernumber"	TEXT NULL,
	"currency"	TEXT NULL,
	"aggregationkey"	TEXT NULL,
	"gamename"	TEXT NULL,
	"highfrequencygame"	TEXT NULL,
	"priceineur"	REAL NULL,
	"feeineur"	REAL NULL,
	"ticketexternalid"	TEXT NULL,
	"winningsineur"	REAL NULL);

CREATE TABLE IF NOT EXISTS "lottery_purchase_stg" (
	"timestampunix"	DATETIME NULL,
	"site"	TEXT NULL,
	"customernumber"	TEXT NULL,
	"currency"	TEXT NULL,
	"amountincents"	REAL NULL,
	"feeamountincents"	REAL NULL,
	"game"	TEXT NULL,
	"orderidentifier"	TEXT NULL,
	"paymentamountincents"	REAL NULL,
	"ticketid"	TEXT NULL,
	"betindex"	TEXT NULL,
	"discount"	TEXT NULL);

-- quarantine tables
CREATE TABLE IF NOT EXISTS "customer_logins_qrtn" (
	"timestamp"	DATETIME NULL,
	"site"	TEXT NULL,
	"customer_id"	TEXT NULL);

CREATE TABLE IF NOT EXISTS "customer_registration_qrtn" (
	"timestamp" DATETIME NULL,
	"site" TEXT NULL,
	"customeremail" TEXT NULL,
	"dateofbirth" DATETIME NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" DATETIME NULL,
	"customernumber" TEXT NULL);

CREATE TABLE IF NOT EXISTS "games_purchase_qrtn" (
	"timestamp"	DATETIME NULL,
	"sitetid"	TEXT NULL,
	"customernumber"	TEXT NULL,
	"currency"	TEXT NULL,
	"aggregationkey"	TEXT NULL,
	"gamename"	TEXT NULL,
	"highfrequencygame"	TEXT NULL,
	"priceineur"	REAL NULL,
	"feeineur"	REAL NULL,
	"ticketexternalid"	TEXT NULL,
	"winningsineur"	REAL NULL);

CREATE TABLE IF NOT EXISTS "lottery_purchase_qrtn" (
	"timestampunix"	DATETIME NULL,
	"site"	TEXT NULL,
	"customernumber"	TEXT NULL,
	"currency"	TEXT NULL,
	"amountincents"	REAL NULL,
	"feeamountincents"	REAL NULL,
	"game"	TEXT NULL,
	"orderidentifier"	TEXT NULL,
	"paymentamountincents"	REAL NULL,
	"ticketid"	TEXT NULL,
	"betindex"	TEXT NULL,
	"discount"	TEXT NULL);

-- clean tables
CREATE TABLE IF NOT EXISTS "customer_logins_clean" (
	"timestamp"	DATETIME NOT NULL,
	"site"	TEXT NOT NULL,
	"customer_id" TEXT NOT NULL,
	"audittime" DATETIME DATETIME('now'));

CREATE TABLE IF NOT EXISTS "customer_registration_clean" (
	"timestamp" DATETIME NOT NULL,
	"site" TEXT NOT NULL,
	"customeremail" TEXT NOT NULL,
	"dateofbirth" DATETIME NOT NULL, 
	"familyname" TEXT NOT NULL,
	"givennames" TEXT NOT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" DATETIME NULL,
	"customernumber" TEXT NOT NULL,
	"audittime" DATETIME DATETIME('now'));

CREATE TABLE IF NOT EXISTS "games_purchase_clean" (
	"timestamp"	DATETIME NOT NULL,
	"sitetid"	TEXT NOT NULL,
	"customernumber"	TEXT NOT NULL,
	"currency"	TEXT NULL,
	"aggregationkey"	TEXT NULL,
	"gamename"	TEXT NOT NULL,
	"highfrequencygame"	TEXT NULL,
	"priceineur"	REAL NOT NULL,
	"feeineur"	REAL NOT NULL,
	"ticketexternalid"	TEXT NOT NULL,
	"winningsineur"	REAL NULL,
	"audittime" DATETIME DATETIME('now'));

CREATE TABLE IF NOT EXISTS "lottery_purchase_clean" (
	"timestamp"	DATETIME NOT NULL,
	"site"	TEXT NOT NULL,
	"customernumber"	TEXT NOT NULL,
	"currency"	TEXT NULL,
	"amountineur"	REAL NOT NULL,
	"feeamountineur"	REAL NOT NULL,
	"game"	TEXT NOT NULL,
	"orderidentifier"	TEXT NOT NULL,
	"paymentamountineur"	REAL NULL,
	"ticketid"	TEXT NOT NULL,
	"betindex"	TEXT NULL,
	"discount"	TEXT NULL,
	"audittime" DATETIME DATETIME('now'));

-- triggers

CREATE TRIGGER IF NOT EXISTS "delete_customer_logins"
	AFTER DELETE ON "customer_logins_clean"
BEGIN
		UPDATE customer_logins_clean SET audittime = datetime('now');
END;

CREATE TRIGGER IF NOT EXISTS "delete_customer_registrations"
	AFTER DELETE ON "customer_registration_clean"
BEGIN
		UPDATE customer_registration_clean SET audittime = datetime('now');
END;

CREATE TRIGGER IF NOT EXISTS "delete_games_purchase"
	AFTER DELETE ON "games_purchase_clean"
BEGIN
		UPDATE games_purchase_clean SET audittime = datetime('now');
END;

CREATE TRIGGER IF NOT EXISTS "delete_lottery_purchase"
	AFTER DELETE ON "lottery_purchase_clean"
BEGIN
		UPDATE lottery_purchase_clean SET audittime = datetime('now');
END;

CREATE TRIGGER IF NOT EXISTS "update_customer_logins"
	AFTER UPDATE ON "customer_logins_clean"
BEGIN
		UPDATE customer_logins_clean SET audittime = DATETIME('now') WHERE OLD.customer_id = NEW.customer_id;
END;

CREATE TRIGGER IF NOT EXISTS "update_customer_registrations"
	AFTER UPDATE ON "customer_registration_clean"
BEGIN
		UPDATE customer_registration_clean SET audittime = DATETIME('now') WHERE OLD.customernumber = NEW.customernumber;
END;

CREATE TRIGGER IF NOT EXISTS "update_games_purchase"
	AFTER UPDATE ON "games_purchase_clean"
BEGIN
		UPDATE games_purchase_clean SET audittime = DATETIME('now') WHERE OLD.ticketexternalid = NEW.ticketexternalid;
END;

CREATE TRIGGER IF NOT EXISTS "update_lottery_purchase"
	AFTER UPDATE ON "lottery_purchase_clean"
BEGIN
		UPDATE lottery_purchase_clean SET audittime = DATETIME('now') WHERE OLD.ticketid = NEW.ticketid;
END;

-- constrains

CREATE INDEX customer_logins_idx ON customer_logins_clean (audittime);
CREATE INDEX customer_registration_idx ON customer_registration_clean (audittime);
CREATE INDEX games_purchase_idx ON games_purchase_clean (audittime);
CREATE INDEX lottery_purchase_idx ON lottery_purchase_clean (audittime);

COMMIT;