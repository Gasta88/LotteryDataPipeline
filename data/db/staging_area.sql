BEGIN TRANSACTION;
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS "audit_events" (
	"last_event" DATETIME NULL);

-- stage tables
CREATE TABLE IF NOT EXISTS "customer_logins_stg" (
	"timestamp" TEXT NULL,
	"site"	TEXT NULL,
	"customernumber"	TEXT NULL);

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
	"customernumber"	TEXT NULL);

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
	"customernumber" TEXT NOT NULL,
	"audittime" DATETIME DEFAULT CURRENT_TIMESTAMP);

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
	"audittime" DATETIME DEFAULT CURRENT_TIMESTAMP);

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
	"audittime" DATETIME DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS "lottery_purchase_clean" (
	"timestampunix"	TEXT NOT NULL,
	"site"	TEXT NOT NULL,
	"customernumber"	TEXT NOT NULL,
	"currency"	TEXT NULL,
	"amountincents"	REAL NOT NULL,
	"feeamountincents"	REAL NOT NULL,
	"game"	TEXT NOT NULL,
	"orderidentifier"	TEXT NOT NULL,
	"paymentamountincents"	REAL NULL,
	"ticketid"	TEXT NOT NULL,
	"betindex"	TEXT NULL,
	"discount"	TEXT NULL,
	"audittime" DATETIME DEFAULT CURRENT_TIMESTAMP);

-- triggers

CREATE TRIGGER IF NOT EXISTS "update_customer_logins"
	AFTER UPDATE ON "customer_logins_clean"
BEGIN
		UPDATE customer_logins_clean SET audittime = datetime('now') WHERE OLD.customernumber = NEW.customernumber;
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

CREATE TRIGGER IF NOT EXISTS "insert_customer_logins"
	AFTER UPDATE ON "customer_logins_clean"
BEGIN
		UPDATE customer_logins_clean SET audittime = datetime('now') WHERE customernumber = NEW.customernumber;
END;

CREATE TRIGGER IF NOT EXISTS "insert_customer_registrations"
	AFTER UPDATE ON "customer_registration_clean"
BEGIN
		UPDATE customer_registration_clean SET audittime = datetime('now') WHERE customernumber = NEW.customernumber;
END;

CREATE TRIGGER IF NOT EXISTS "insert_games_purchase"
	AFTER UPDATE ON "games_purchase_clean"
BEGIN
		UPDATE games_purchase_clean SET audittime = datetime('now') WHERE ticketexternalid = NEW.ticketexternalid;
END;

CREATE TRIGGER IF NOT EXISTS "insert_lottery_purchase"
	AFTER UPDATE ON "lottery_purchase_clean"
BEGIN
		UPDATE lottery_purchase_clean SET audittime = datetime('now') WHERE ticketid = NEW.ticketid;
END;

-- constrains

CREATE INDEX customer_logins_idx ON customer_logins_clean (audittime);
CREATE INDEX customer_registration_idx ON customer_registration_clean (audittime);
CREATE INDEX games_purchase_idx ON games_purchase_clean (audittime);
CREATE INDEX lottery_purchase_idx ON lottery_purchase_clean (audittime);

-- views

CREATE VIEW IF NOT EXISTS customer_vw AS 
SELECT  customernumber AS id,
		customeremail AS email,
		dateofbirth,
		familyname,
		givennames,
		primaryaddress_addressline  AS addressline,
		primaryaddress_city AS city,
		primaryaddress_federalstate AS federalstate,
		primaryaddress_postalcode AS postalcode,
		primaryaddress_sovereignstate AS sovereignstate,
		primaryaddress_street AS street,
		audittime 
	FROM customer_registration_clean 
	WHERE audittime >= (SELECT last_event FROM audit_events);

CREATE VIEW IF NOT EXISTS discount_vw AS 
SELECT  ROW_NUMBER () OVER ( 
        ORDER BY filtered_tbl.discount 
    ) AS id,
    filtered_tbl.discount,
    filtered_tbl.audittime
	FROM (SELECT DISTINCT(discount), audittime FROM lottery_purchase_clean 
	WHERE discount IS NOT NULL AND audittime >= (SELECT last_event FROM audit_events)) filtered_tbl; 

CREATE VIEW IF NOT EXISTS login_vw AS 
SELECT customernumber AS id,
	   site AS website,
	   "timestamp",
	   audittime 
	FROM customer_logins_clean
	WHERE audittime >= (SELECT last_event FROM audit_events);
	   

CREATE VIEW IF NOT EXISTS registration_vw AS 
SELECT ROW_NUMBER () OVER ( 
        ORDER BY customernumber 
    	) as id,
       site AS website,
       "timestamp" AS dateofregistration,
       customernumber AS customer_id,
       audittime
   FROM customer_registration_clean
   WHERE audittime >= (SELECT last_event FROM audit_events);

CREATE VIEW IF NOT EXISTS product_vw AS 
SELECT ROW_NUMBER () OVER ( 
        ORDER BY union_tbl.name 
    	) AS id,
       union_tbl.name,
	   union_tbl.product_type AS "type",
	   audittime 
	FROM (SELECT DISTINCT(LOWER(game)) AS name, 'lottery_game' AS "product_type", audittime FROM lottery_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)
			UNION
		  SELECT DISTINCT(LOWER(gamename)) AS name, 'instant_game' AS "product_type", audittime FROM games_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)) AS union_tbl;
  	 
CREATE VIEW IF NOT EXISTS ticket_vw AS 
SELECT union_tbl.id,
	   product_vw.id AS product_id,
	   union_tbl.betindex,
	   union_tbl.currency,
	   union_tbl.price,
	   union_tbl.fee,
	   discount_vw.id AS discount_id,
	   union_tbl.audittime
	FROM (SELECT ticketid || orderidentifier || customernumber AS id,
	   lower(game) AS name,
	   betindex,
	   currency,
	   (amountincents / 100) AS price,
	   (feeamountincents / 100) AS fee,
	   discount,
	   audittime FROM 
	   lottery_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)
	UNION ALL
	SELECT aggregationkey || customernumber || ticketexternalid || "timestamp" AS id,
	   lower(gamename) AS name,
	   NULL AS betindex,
	   currency,
	   priceineur AS price,
	   feeineur AS fee,
	   NULL AS discount,
	   audittime from games_purchase_clean 
	  WHERE audittime >= (SELECT last_event FROM audit_events)) AS 
	 union_tbl LEFT JOIN product_vw ON union_tbl.name = product_vw.name
				LEFT JOIN discount_vw ON union_tbl.discount = discount_vw.discount;	 

CREATE VIEW IF NOT EXISTS booking_vw AS 
SELECT union_tbl.order_id,
       union_tbl.customer_id,
	   union_tbl.ticket_id,
	   union_tbl."timestamp",
	   union_tbl.website,
	   union_tbl.audittime
	FROM (SELECT orderidentifier AS order_id,
	   ticketid || orderidentifier || customernumber AS ticket_id,
	   customernumber AS customer_id,
	   datetime(timestampunix, 'unixepoch', 'localtime') as "timestamp",
	   site as website,
	   audittime 
	   FROM lottery_purchase_clean WHERE audittime >= (SELECT last_event FROM audit_events)
	UNION ALL
	SELECT ticketexternalid AS order_id,
		aggregationkey || customernumber || ticketexternalid || "timestamp" as ticket_id,
		customernumber as customer_id,
		"timestamp",
		sitetid as website,
		audittime 
		FROM games_purchase_clean
	  WHERE audittime >= (SELECT last_event FROM audit_events)) AS 
	 union_tbl;

COMMIT;