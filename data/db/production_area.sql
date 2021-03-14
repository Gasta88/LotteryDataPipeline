BEGIN TRANSACTION;
PRAGMA user_version = 1;
PRAGMA foreign_keys = ON;

-- production tables
CREATE TABLE IF NOT EXISTS "customer" (
	"id" TEXT PRIMARY KEY,
	"email" TEXT NOT NULL,
	"dateofbirth" DATETIME NOT NULL,
	"familyname" TEXT NULL,
	"givennames" TEXT NULL,
	"addressline" TEXT NULL,
	"city" TEXT NULL,
	"federalstate" TEXT NULL,
	"postalcode" TEXT NULL,
	"sovereignstate" TEXT NULL,
	"street" TEXT NULL
);

CREATE TABLE IF NOT EXISTS "discount"(
	"id" INTEGER PRIMARY KEY,
	"category" TEXT NOT NULL,
	"value" REAL NOT NULL,
	"type" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "login"(
	"id" INTEGER PRIMARY KEY,
	"customer_id" TEXT NOT NULL,
	"website" TEXT NOT NULL,
	"timestamp" DATETIME NOT NULL,
	FOREIGN KEY ("customer_id") 
		REFERENCES "customer"("id") 
			ON UPDATE CASCADE
			ON DELETE SET NULL -- maintain login history of inactive customers
);

CREATE TABLE IF NOT EXISTS "registration"(
	"id" INTEGER PRIMARY KEY,
	"website" TEXT NOT NULL,
	"dateofregistration" DATETIME NOT NULL,
	"customer_id" TEXT NOT NULL,
	FOREIGN KEY ("customer_id")
		REFERENCES "customer"("id")
			ON UPDATE CASCADE
			ON DELETE SET NULL -- leaving customers are inactive
);

CREATE TABLE IF NOT EXISTS "product"(
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL,
	"type" TEXT NOT NULL -- either lottery or instant game
);


CREATE TABLE IF NOT EXISTS "ticket"(
	"id" TEXT PRIMARY KEY,  
	"product_id" TEXT NOT NULL,
	"betindex" INTEGER NOT NULL,
	"currency"	TEXT NULL,
	"price" REAL DEFAULT 0.0,
	"fee" REAL DEFAULT 0.0,
	"discount_id" INTEGER NOT NULL,	
	FOREIGN KEY ("product_id")
		REFERENCES "product"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE, -- discontinued products invalidate tickets
	FOREIGN KEY ("discount_id")
		REFERENCES "discount"("id")
			ON UPDATE CASCADE
			ON DELETE SET NULL 
);


CREATE TABLE IF NOT EXISTS "booking"(
	"order_id" TEXT NOT NULL,
	"customer_id" TEXT NOT NULL,
	"ticket_id" TEXT NOT NULL,
	"timestamp" DATETIME NOT NULL,
	"website" TEXT NOT NULL,
	FOREIGN KEY ("customer_id")
		REFERENCES "customer"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE, -- inactive customers do not maintain their booking history
	FOREIGN KEY ("ticket_id")
		REFERENCES "ticket"("id")
			ON UPDATE CASCADE
			ON DELETE CASCADE -- discontinued tickets invalidate products
);

-- views


COMMIT;