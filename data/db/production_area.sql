BEGIN TRANSACTION;
PRAGMA user_version = 1;
PRAGMA foreign_keys = ON;

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
	FOREIGN KEY ("customer_id") 
		REFERENCES "customer"("id") 
			ON UPDATE CASCADE
			ON DELETE CASCADE,
	PRIMARY KEY ("id", AUTOINCREMENT)
);

CREATE TABLE IF NOT EXISTS "registration"(
	"id" INTEGER NOT NULL UNIQUE,
	"website" TEXT NOT NULL,
	"dateofregistration" DATETIME NOT NULL,
	"customer_id" TEXT NOT NULL,
	PRIMARY KEY ("id", AUTOINCREMENT),
	FOREIGN KEY ("customer_id")
		REFERENCES "customer"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE
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
	FOREIGN KEY ("customer_id")
		REFERENCES "customer"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE,
	FOREIGN KEY ("product_id")
		REFERENCES "product"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE,
	FOREIGN KEY ("discount_id")
		REFERENCES "discount"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE
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
	FOREIGN KEY ("customer_id")
		REFERENCES "customer"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE,
	FOREIGN KEY ("product_id")
		REFERENCES "product"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE
);

-- views


COMMIT;