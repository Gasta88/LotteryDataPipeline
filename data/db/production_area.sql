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
	"id" TEXT NOT NULL,
	"website" TEXT NOT NULL,
	"timestamp" DATETIME NOT NULL,
	FOREIGN KEY ("id") 
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
	"betindex" INTEGER NULL,
	"currency"	TEXT NULL,
	"price" REAL DEFAULT 0.0,
	"fee" REAL DEFAULT 0.0,
	"discount_id" INTEGER NULL,	
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

CREATE VIEW IF NOT EXISTS  "billing_vw" AS
SELECT
	p.id AS product_id,
	p.name,
	p."type" ,
	b.website,
	b."timestamp" AS booking_datetime,
	STRFTIME('%Y', b."timestamp") AS booking_year,
	STRFTIME('%m', b."timestamp") AS booking_month,
	STRFTIME('%d', b."timestamp") AS booking_day,
	CASE
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 1 AND 3 THEN 1
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 4 and 6 THEN 2
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 7 and 9 THEN 3
		ELSE 4
	END AS booking_quarter,
	t.price + t.fee AS total_price,
	t.currency
FROM
	booking b
INNER JOIN ticket t ON
	b.ticket_id = t.id
INNER JOIN product p ON
	t.product_id = p.id;

CREATE VIEW IF NOT EXISTS "active_customers_vw" AS
SELECT c.id AS customer_id,
       l."timestamp" AS login_datetime,
       STRFTIME('%Y', l."timestamp") AS login_year,
	   STRFTIME('%m', l."timestamp") AS login_month,
	   STRFTIME('%W', l."timestamp") AS login_week,
	   STRFTIME('%d', l."timestamp") AS login_day,
		CASE
			WHEN CAST(strftime('%m', l."timestamp") AS integer) BETWEEN 1 AND 3 THEN 1
			WHEN CAST(strftime('%m', l."timestamp") AS integer) BETWEEN 4 and 6 THEN 2
			WHEN CAST(strftime('%m', l."timestamp") AS integer) BETWEEN 7 and 9 THEN 3
			ELSE 4
		END AS login_quarter,
		l.website 
FROM customer c INNER JOIN login l ON c.id=l.id;

CREATE VIEW IF NOT EXISTS "checkout_vw" AS
SELECT c.id AS customer_id,
	   b.order_id,
	   b."timestamp" AS booking_datetime,
	STRFTIME('%Y', b."timestamp") AS booking_year,
	STRFTIME('%m', b."timestamp") AS booking_month,
	STRFTIME('%d', b."timestamp") AS booking_day,
	CASE
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 1 AND 3 THEN 1
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 4 and 6 THEN 2
		WHEN CAST(strftime('%m', b."timestamp") AS integer) BETWEEN 7 and 9 THEN 3
		ELSE 4
	END AS booking_quarter,
	t.price + t.fee AS total_price,
	t.currency,
	b.website 
FROM booking b INNER JOIN ticket t ON b.ticket_id =t.id
				INNER JOIN customer c ON b.customer_id =c.id;
COMMIT;