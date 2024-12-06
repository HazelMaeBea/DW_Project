TRUNCATE TABLE cleaned;
TRUNCATE TABLE for_cleaning;
TRUNCATE TABLE invalid;

SELECT * FROM landing_table;
SELECT * FROM cleaned;
SELECT * FROM for_cleaning;
SELECT * FROM invalid;

SELECT * FROM for_cleaning
WHERE order_id = '150925';


CREATE TABLE IF NOT EXISTS cleaned (
    order_id INT,
    product VARCHAR(255),
    quantity_ordered INT,
    price_each DECIMAL(10, 2),
    order_date TIMESTAMP,
    purchase_address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS for_cleaning (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS invalid (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
);

-- Stored procedure for data mapping
CREATE OR REPLACE PROCEDURE data_mapping()
LANGUAGE plpgsql
AS $$
BEGIN

	CREATE TABLE IF NOT EXISTS cleaned (
    order_id INT,
    product VARCHAR(255),
    quantity_ordered INT,
    price_each DECIMAL(10, 2),
    order_date TIMESTAMP,
    purchase_address VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS for_cleaning (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS invalid (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
	);
    
    INSERT INTO invalid
    SELECT * FROM landing_table
    WHERE order_id IS NULL 
        OR product IS NULL 
        OR quantity_ordered IS NULL
        OR price_each IS NULL
        OR order_date IS NULL 
        OR purchase_address IS NULL
        OR LOWER(order_id) = 'order id'
        OR LOWER(product) = 'product name'
        OR LOWER(quantity_ordered) = 'quantity ordered'
        OR LOWER(price_each) = 'price each'
        OR LOWER(order_date) = 'order date'
        OR LOWER(purchase_address) = 'purchase address';
    
    INSERT INTO cleaned
    SELECT
        order_id::INT,
        product,
        quantity_ordered::INT,
        price_each::DECIMAL(10, 2),
        TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI:SS PM') AS order_date,
        purchase_address
    FROM landing_table a
    WHERE order_id IS NOT NULL
      AND product IS NOT NULL
      AND quantity_ordered IS NOT NULL
      AND price_each IS NOT NULL
      AND order_date IS NOT NULL
      AND purchase_address IS NOT NULL
      AND quantity_ordered ~ '^[1-9][0-9]*$' -- Checks if quantity_ordered is a positive integer
      AND price_each ~ '^[0-9]+\.[0-9]{2}$' -- Checks if price_each is a valid decimal number
      AND NOT EXISTS (
          SELECT 1
          FROM landing_table b
          WHERE a.order_id = b.order_id
          GROUP BY b.order_id
          HAVING COUNT(*) > 1
		  -- to catch duplicates
      );

    -- Insert remaining values into for_cleaning table
    INSERT INTO for_cleaning
    SELECT *
    FROM landing_table a
    WHERE NOT EXISTS (
        SELECT 1
        FROM cleaned b
        WHERE a.order_id = b.order_id::VARCHAR
    )
    AND order_id IS NOT NULL 
    AND product IS NOT NULL 
    AND quantity_ordered IS NOT NULL
    AND price_each IS NOT NULL
    AND order_date IS NOT NULL 
    AND purchase_address IS NOT NULL
    AND LOWER(order_id) != 'order id'
    AND LOWER(product) != 'product name'
    AND LOWER(quantity_ordered) != 'quantity ordered'
    AND LOWER(price_each) != 'price each'
    AND LOWER(order_date) != 'order date'
    AND LOWER(purchase_address) != 'purchase address';
END;
$$;

-- Stored procedure for data versioning
CREATE OR REPLACE PROCEDURE data_versioning()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Add versioning logic here i have no clue pa
END;
$$;


TRUNCATE TABLE cleaned;
TRUNCATE TABLE for_cleaning;
TRUNCATE TABLE invalid;

SELECT * FROM landing_table;
SELECT * FROM cleaned;
SELECT * FROM for_cleaning;
SELECT * FROM invalid;

SELECT * FROM for_cleaning
WHERE order_id = '150925';
SELECT * FROM processing ORDER BY order_id;


-- Stored procedure for data cleansing
CREATE OR REPLACE PROCEDURE data_cleansing()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Fix to 2 decimal points
    UPDATE for_cleaning
    SET price_each = TO_CHAR(ROUND(CAST(price_each AS NUMERIC), 2), 'FM999999999.00')
    WHERE price_each ~ '^[0-9]+(\.[0-9]{1,2})?$';

	-- Insert into CLEANED table one instance of complete duplicate
	INSERT INTO cleaned
	SELECT 
	DISTINCT ON (order_id, product, quantity_ordered, price_each, order_date, purchase_address) 
	order_id::INT, 
	product, 
	quantity_ordered::INT, 
	price_each::DECIMAL(10, 2), 
	TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI:SS PM') AS order_date,
	purchase_address
	FROM for_cleaning
	WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address, 'T') IN (
		SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, purchase_address,
			   CASE WHEN COUNT(*) > 1 THEN 'T'
			   ELSE 'F' END
		FROM public.for_cleaning
		GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
		HAVING COUNT(*) > 1
		ORDER BY order_id, product
	);

	-- Insert into INVALID table other instance of complete duplicate
	INSERT INTO invalid
	SELECT 
	DISTINCT ON (order_id, product, quantity_ordered, price_each, order_date, purchase_address) 
	order_id, 
	product, 
	quantity_ordered, 
	price_each 
	order_date,
	purchase_address
	FROM for_cleaning
	WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address, 'T') IN (
		SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, purchase_address,
			   CASE WHEN COUNT(*) > 1 THEN 'T'
			   ELSE 'F' END
		FROM public.for_cleaning
		GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
		HAVING COUNT(*) > 1
		ORDER BY order_id, product
	);

	-- Delete records from FOR_CLEANING after insertion to cleaned and invalid
	DELETE FROM for_cleaning
	WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) IN (
	  SELECT DISTINCT ON (order_id, product, quantity_ordered, price_each, order_date, purchase_address)
	         order_id, 
	         product, 
	         quantity_ordered, 
	         price_each, 
	         order_date, 
	         purchase_address
	  FROM for_cleaning
	  WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address, 'T') IN (
	    SELECT order_id, 
	           product, 
	           quantity_ordered, 
	           price_each, 
	           order_date, 
	           purchase_address,
	           CASE WHEN COUNT(*) > 1 THEN 'T'
	                ELSE 'F' END
	    FROM for_cleaning
	    GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
	    HAVING COUNT(*) > 1
	  )
	);

	-- Insert non-duplicate records from the for processing
	INSERT INTO cleaned
	SELECT 
		order_id::INT, 
		product, 
		quantity_ordered::INT, 
		price_each::DECIMAL(10, 2), 
		TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI:SS PM') AS order_date,
		purchase_address
	FROM for_cleaning
	WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) NOT IN (
		SELECT * FROM for_cleaning WHERE order_id IN (
			SELECT order_id
			FROM for_cleaning
			GROUP BY order_id
			HAVING COUNT(*) > 1
		)
	)

	-- Delete non-duplicate records from the for processing
	DELETE FROM for_cleaning
	WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) NOT IN (
		SELECT * FROM for_cleaning WHERE order_id IN (
			SELECT order_id
			FROM for_cleaning
			GROUP BY order_id
			HAVING COUNT(*) > 1
		)	
	)

	--insert code here to handle the duplicates currently in the for_cleaning table
	

END;
$$;

-- Normalization logic
CREATE OR REPLACE FUNCTION normalize_data() RETURNS VOID AS 
LANGUAGE plpgsql
$$
BEGIN
    -- Example normalization logic
    INSERT INTO cleaned_normalized 
        (order_id, product, quantity_ordered, price_each, order_date, month, day, year, hour, minute, number, street, city, state, zip_code)
    SELECT 
        order_id, 
        product, 
        quantity_ordered, 
        price_each, 
        order_date, 
        EXTRACT(MONTH FROM order_date) AS month, 
        EXTRACT(DAY FROM order_date) AS day, 
        EXTRACT(YEAR FROM order_date) AS year, 
        EXTRACT(HOUR FROM order_date) AS hour, 
        EXTRACT(MINUTE FROM order_date) AS minute, 
        SPLIT_PART(purchase_address, ' ', 1) AS number, 
        SPLIT_PART(purchase_address, ' ', 2) AS street, 
        SPLIT_PART(purchase_address, ',', 2) AS city, 
        SPLIT_PART(purchase_address, ',', 3) AS state, 
        SPLIT_PART(purchase_address, ' ', 4) AS zip_code
    FROM cleaned;
END;
$$;

-- Call the procedures
CALL data_mapping();
CALL data_versioning();
CALL data_cleansing();
CALL normalize_data();

SELECT * FROM cleaned

SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, purchase_address,
			   CASE WHEN COUNT(*) > 1 THEN 'T'
			   ELSE 'F' END
		FROM cleaned
		GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
		-- HAVING COUNT(*) > 1
		ORDER BY order_id, product