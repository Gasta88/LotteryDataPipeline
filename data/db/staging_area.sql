BEGIN TRANSACTION;
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS "audit_events" (
	"last_event" DATETIME NULL);

-- stage tables
CREATE TABLE IF NOT EXISTS "customer_logins_stg" (
	"timestamp" TEXT NULL,
	"site"	TEXT NULL,
	"customer_id"	TEXT NULL);

CREATE TABLE IF NOT EXISTS "customer_registration_stg" (
	"timestamp" TEXT NULL,
	"site" TEXT NULL,
	"customeremail" TEXT NULL,
	"dateofbirth" TEXT NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" TEXT NULL,
	"customernumber" TEXT NULL);

CREATE TABLE IF NOT EXISTS "games_purchase_stg" (
	"timestamp" TEXT NULL,
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
	"timestampunix"	TEXT NULL,
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
	"timestamp" TEXT NULL,
	"site"	TEXT NULL,
	"customer_id"	TEXT NULL);

CREATE TABLE IF NOT EXISTS "customer_registration_qrtn" (
	"timestamp" TEXT NULL,
	"site" TEXT NULL,
	"customeremail" TEXT NULL,
	"dateofbirth" TEXT NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" TEXT NULL,
	"customernumber" TEXT NULL);

CREATE TABLE IF NOT EXISTS "games_purchase_qrtn" (
	"timestamp" TEXT NULL,
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
	"timestampunix"	TEXT NULL,
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
	"timestamp"	TEXT NOT NULL,
	"site"	TEXT NOT NULL,
	"customer_id" TEXT NOT NULL,
	"audittime" DATETIME DEFAULT current_timestamp);

CREATE TABLE IF NOT EXISTS "customer_registration_clean" (
	"timestamp" TEXT NOT NULL,
	"site" TEXT NOT NULL,
	"customeremail" TEXT NOT NULL,
	"dateofbirth" TEXT NOT NULL, 
	"familyname" TEXT NOT NULL,
	"givennames" TEXT NOT NULL,
	"primaryaddress_addressline" TEXT NULL,
	"primaryaddress_city" TEXT NULL,
	"primaryaddress_federalstate" TEXT NULL,
	"primaryaddress_postalcode" TEXT NULL,
	"primaryaddress_sovereignstate" TEXT NULL,
	"primaryaddress_street" TEXT NULL,
	"registrationdate" TEXT NULL,
	"customernumber" TEXT NOT NULL,
	"audittime" DATETIME DEFAULT current_timestamp);

CREATE TABLE IF NOT EXISTS "games_purchase_clean" (
	"timestamp"	TEXT NOT NULL,
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
	"audittime" DATETIME DEFAULT current_timestamp);

CREATE TABLE IF NOT EXISTS "lottery_purchase_clean" (
	"timestamp"	TEXT NOT NULL,
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
	"audittime" DATETIME DEFAULT current_timestamp);

-- triggers

CREATE TRIGGER IF NOT EXISTS "update_customer_logins"
	AFTER UPDATE ON "customer_logins_clean"
BEGIN
		UPDATE customer_logins_clean SET audittime = datetime('now') WHERE OLD.customer_id = NEW.customer_id;
END;

CREATE TRIGGER IF NOT EXISTS "update_customer_registrations"
	AFTER UPDATE ON "customer_registration_clean"
BEGIN
		UPDATE customer_registration_clean SET audittime = datetime('now') WHERE OLD.customernumber = NEW.customernumber;
END;

CREATE TRIGGER IF NOT EXISTS "update_games_purchase"
	AFTER UPDATE ON "games_purchase_clean"
BEGIN
		UPDATE games_purchase_clean SET audittime = datetime('now') WHERE OLD.ticketexternalid = NEW.ticketexternalid;
END;

CREATE TRIGGER IF NOT EXISTS "update_lottery_purchase"
	AFTER UPDATE ON "lottery_purchase_clean"
BEGIN
		UPDATE lottery_purchase_clean SET audittime = datetime('now') WHERE OLD.ticketid = NEW.ticketid;
END;

-- constrains

CREATE INDEX customer_logins_idx ON customer_logins_clean (audittime);
CREATE INDEX customer_registration_idx ON customer_registration_clean (audittime);
CREATE INDEX games_purchase_idx ON games_purchase_clean (audittime);
CREATE INDEX lottery_purchase_idx ON lottery_purchase_clean (audittime);

-- views

CREATE VIEW IF NOT EXISTS customer_vw AS 
SELECT  customernumber AS id,
		email,
		dateofbirth,
		familyname,
		givennames,
		addressline,
		city,
		federalstate,
		postalcode,
		sovereignstate,
		street
	FROM customer_registration_clean 
	WHERE audittime >= (SELECT last_event FROM audit_events);


CREATE VIEW IF NOT EXISTS discount_vw AS 
SELECT DISTINCT(discount) 
	FROM lottery_purchase_clean 
	WHERE discount IS NOT NULL AND 
		  audittime >= (SELECT last_event FROM audit_events);

CREATE VIEW IF NOT EXISTS login_vw AS 
SELECT customer_id,
	   site AS website,
	   "timestamp"
	FROM customer_logins_clean
	WHERE audittime >= (SELECT last_event FROM audit_events);
	   

CREATE VIEW IF NOT EXISTS registration_vw AS 
SELECT site AS website,
       "timestamp" AS dateofregistration,
       customernumber AS customer_id
   FROM customer_registration_clean
   WHERE audittime >= (SELECT last_event FROM audit_events);

CREATE VIEW IF NOT EXISTS product_vw AS 
SELECT union_tbl.name,
	   union_tbl. AS "type" 
	FROM (SELECT DISTINCT(LOWER(game)), 'lottery_game' AS "product_type" FROM lottery_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)
			UNION
		  SELECT DISTINCT(LOWER(gamename)), 'instant_game' AS "product_type" FROM games_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)) AS union_tbl;
  
  

		 
CREATE VIEW IF NOT EXISTS ticket_vw AS 
SELECT		 

CREATE TABLE IF NOT EXISTS "ticket"(
	"id" TEXT NOT NULL UNIQUE,  --ticket_id for lottery, ??? for games
	"product_id" TEXT NOT NULL,
	"betindex" INTEGER NOT NULL,
	"currency"	TEXT NULL,
	"price" REAL DEFAULT 0.0,
	"fee" REAL DEFAULT 0.0,
	"discount_id" INTEGER NOT NULL,	
	PRIMARY KEY ("id"),
	FOREIGN KEY ("product_id")
		REFERENCES "product"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE, -- discontinued products invalidate tickets
	FOREIGN KEY ("discount_id")
		REFERENCES "discount"("id")
			ON UPDATE CASCADE
			ON DELETE SET NULL 
);

COMMIT;