-- TRUNCATE TABLE landing;
CREATE TABLE IF NOT EXISTS landing_table (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
);
-- SELECT * FROM landing_table;

-- TRUNCATE TABLE cleaned;
CREATE TABLE IF NOT EXISTS cleaned (
    order_id INT,
    product VARCHAR(255),
    quantity_ordered INT,
    price_each DECIMAL(10, 2),
    order_date TIMESTAMP,
    purchase_address VARCHAR(255)
);
-- SELECT * FROM cleaned;

-- TRUNCATE TABLE for_cleaning;
CREATE TABLE IF NOT EXISTS for_cleaning (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
);
-- SELECT * FROM for_cleaning;

-- TRUNCATE TABLE invalid;
CREATE TABLE IF NOT EXISTS invalid (
    order_id VARCHAR(255),
    product VARCHAR(255),
    quantity_ordered VARCHAR(255),
    price_each VARCHAR(255),
    order_date VARCHAR(255),
    purchase_address VARCHAR(255)
);
-- SELECT * FROM invalid;

-- Stored procedure for data mapping
CREATE OR REPLACE PROCEDURE data_mapping()
LANGUAGE plpgsql
AS $$
BEGIN
    -- pag may null, invalid
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
    
    -- Insert into cleaned table
    INSERT INTO cleaned
    SELECT
        order_id::INT,
        product, --is already type varchar
        quantity_ordered::INT,
        price_each::DECIMAL(10, 2),
        TO_TIMESTAMP(order_date, 'MM/DD/YYYY HH24:MI:SS PM') AS order_date, --will not insert invalid timestamp
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

-- Stored procedure for data cleansing
CREATE OR REPLACE PROCEDURE data_cleansing()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Fix to 2 decimal points
    UPDATE for_cleaning
    SET price_each = TO_CHAR(ROUND(CAST(price_each AS NUMERIC), 2), 'FM999999999.00')
    WHERE price_each ~ '^[0-9]+(\.[0-9]{1,2})?$';

    -- Find and remove duplicate rows
    WITH duplicates AS (
        SELECT
            order_id,
            product,
            quantity_ordered,
            price_each,
            order_date,
            purchase_address,
            COUNT(*) AS cnt
        FROM for_cleaning
        GROUP BY
            order_id,
            product,
            quantity_ordered,
            price_each,
            order_date,
            purchase_address
        HAVING COUNT(*) > 1
    )
    DELETE FROM for_cleaning
    WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) IN (
        SELECT order_id, product, quantity_ordered, price_each, order_date, purchase_address
        FROM duplicates
    );

    -- Insert duplicates into invalid table
    INSERT INTO invalid
    SELECT *
    FROM duplicates;
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
