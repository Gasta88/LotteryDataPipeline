BEGIN TRANSACTION;
PRAGMA user_version = 1;

-- production tables
CREATE TABLE IF NOT EXISTS "customer" (
	"id" TEXT NOT NULL UNIQUE,
	"email" TEXT NOT NULL,
	"dateofbirth" DATETIME NOT NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"addressline" TEXT NULL,
	"city" TEXT NULL,
	"federalstate" TEXT NULL,
	"postalcode" TEXT NULL,
	"sovereignstate" TEXT NULL,
	"street" TEXT NULL,
	PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "discount"(
	"id" INTEGER NOT NULL UNIQUE,
	"category" TEXT NOT NULL,
	"value" REAL NOT NULL,
	"type" TEXT NOT NULL,
	PRIMARY KEY ("id", AUTOINCREMENT)
);

CREATE TABLE IF NOT EXISTS "login"(
	"id" INTEGER NOT NULL UNIQUE,
	"customer_id" TEXT NOT NULL,
	"website" TEXT NOT NULL,
	"timestamp" DATETIME NOT NULL,
	FOREIGN KEY ("customer_id") REFERENCES "customer"("id"),
	PRIMARY KEY ("id", AUTOINCREMENT)
);

CREATE TABLE IF NOT EXISTS "registration"(
	"id" INTEGER NOT NULL UNIQUE,
	"website" TEXT NOT NULL,
	"dateofregistration" DATETIME NOT NULL,
	"customer_id" TEXT NOT NULL,
	PRIMARY KEY ("id", AUTOINCREMENT),
	FOREIGN KEY ("customer_id") REFERENCES "customer"("id")
);

CREATE TABLE IF NOT EXISTS "product"(
	"id" INTEGER NOT NULL UNIQUE,
	"type" TEXT DEFAULT "NA",
	"name" TEXT NOT NULL,
	"currency" TEXT "EUR",
	PRIMARY KEY ("id", AUTOINCREMENT)
);

CREATE TABLE IF NOT EXISTS "ticket"(
	"id" INTEGER NOT NULL UNIQUE,
	"customer_id" TEXT NOT NULL,
	"product_id" INTEGER NOT NULL,
	"betindex" INTEGER NOT NULL,
	"website" TEXT NOT NULL,
	"price" REAL DEFAULT 0.0,
	"fee" REAL DEFAULT 0.0,
	"discount_id" INTEGER NOT NULL,	
	PRIMARY KEY ("id"),
	FOREIGN KEY ("customer_id") REFERENCES "customer"("id"),
	FOREIGN KEY ("product_id") REFERENCES "product"("id")
	FOREIGN KEY ("discount_id") REFERENCES "discount"("id")
);

CREATE TABLE IF NOT EXISTS "booking"(
	"id" INTEGER NOT NULL UNIQUE,
	"customer_id" TEXT NOT NULL,
	"timestamp" DATETIME NOT NULL,
	"website" TEXT NOT NULL,
	"product_id" INTEGER NOT NULL,
	"aggregationkey" TEXT NOT NULL,
	"price" REAL DEFAULT 0.0,
	"fee" REAL DEFAULT 0.0,
	PRIMARY KEY ("id"),
	FOREIGN KEY ("customer_id") REFERENCES "customer"("id"),
	FOREIGN KEY ("product_id") REFERENCES "product"("id")
);

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