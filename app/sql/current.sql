TRUNCATE TABLE cleaned;
TRUNCATE TABLE for_cleaning;
TRUNCATE TABLE invalid;
TRUNCATE TABLE cleaned_normalized;

SELECT * FROM landing_table;
SELECT * FROM cleaned;
SELECT * FROM for_cleaning;
SELECT * FROM invalid;
SELECT * FROM cleaned_normalized;

DROP TABLE cleaned_normalized

-- Call the procedures
CALL data_mapping();
CALL data_cleansing();
CALL normalize_data();
CALL data_versioning();
CALL product_dimension();

--testing for duplicates, both complete and only ids
SELECT * FROM for_cleaning
WHERE order_id = '150925';

-- checking for duplicates in cleaned table
SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, purchase_address,
			   CASE WHEN COUNT(*) > 1 THEN 'T'
			   ELSE 'F' END
		FROM cleaned
		GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
		-- HAVING COUNT(*) > 1
		ORDER BY order_id, product;

-- checking for duplicates in cleaned_normalized table
SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, street, city, state, zip_code,
			   CASE WHEN COUNT(*) > 1 THEN 'T'
			   ELSE 'F' END
		FROM cleaned_normalized
		GROUP BY order_id, product, quantity_ordered, price_each, order_date, street, city, state, zip_code
		-- HAVING COUNT(*) > 1
		ORDER BY order_id, product;

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
        TO_TIMESTAMP(order_date, 'MM/DD/YY HH24:MI:SS') AS order_date,
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
        TO_TIMESTAMP(order_date, 'MM/DD/YY HH24:MI:SS') AS order_date,
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
        price_each, 
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
        TO_TIMESTAMP(order_date, 'MM/DD/YY HH24:MI:SS') AS order_date,
        purchase_address
    FROM for_cleaning
    WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) NOT IN (
        SELECT * FROM for_cleaning WHERE order_id IN (
            SELECT order_id
            FROM for_cleaning
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    );

    -- Delete non-duplicate records from the for processing
    DELETE FROM for_cleaning
    WHERE (order_id, product, quantity_ordered, price_each, order_date, purchase_address) NOT IN (
        SELECT * FROM for_cleaning WHERE order_id IN (
            SELECT order_id
            FROM for_cleaning
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )    
    );

    -- Handle duplicate IDs with different products or quantities
    WITH row_numbers AS (
        SELECT ctid, order_id, ROW_NUMBER() OVER (ORDER BY ctid) AS row_num
        FROM for_cleaning
        WHERE order_id IN (
            SELECT order_id
            FROM for_cleaning
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    )
    UPDATE for_cleaning
    SET order_id = (SELECT MAX(order_id) + row_num
                    FROM cleaned)
    FROM row_numbers
    WHERE for_cleaning.ctid = row_numbers.ctid;

    -- Insert the updated records into the cleaned table
    INSERT INTO cleaned
    SELECT 
        order_id::INT, 
        product, 
        quantity_ordered::INT, 
        price_each::DECIMAL(10, 2), 
        TO_TIMESTAMP(order_date, 'MM/DD/YY HH24:MI:SS') AS order_date,
        purchase_address
    FROM for_cleaning;

    -- Trim data in the cleaned table
    UPDATE cleaned
    SET 
        product = TRIM(BOTH FROM product),
        purchase_address = TRIM(BOTH FROM purchase_address);

    -- Clean up
    TRUNCATE for_cleaning;
END;
$$;

-- Normalization logic
CREATE OR REPLACE PROCEDURE normalize_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create the cleaned_normalized table with appropriate data types
    CREATE TABLE IF NOT EXISTS cleaned_normalized (
        order_id INT,
        product VARCHAR(255),
        quantity_ordered INT,
        price_each DECIMAL(10, 2),
        order_date TIMESTAMP,
        year INT,
        month INT,
        day INT,
        hour INT,
        minute INT,
        street VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        zip_code VARCHAR(255)
    );

    -- Insert data into cleaned_normalized
    INSERT INTO cleaned_normalized 
        (order_id, product, quantity_ordered, price_each, order_date, year, month, day, hour, minute, street, city, state, zip_code)
    SELECT 
        order_id, 
        product, 
        quantity_ordered, 
        price_each, 
        order_date, 
        EXTRACT(YEAR FROM order_date) AS year, 
        EXTRACT(MONTH FROM order_date) AS month, 
        EXTRACT(DAY FROM order_date) AS day, 
        EXTRACT(HOUR FROM order_date) AS hour, 
        EXTRACT(MINUTE FROM order_date) AS minute, 
        SPLIT_PART(purchase_address, ',', 1) AS street, 
        SPLIT_PART(purchase_address, ',', 2) AS city, 
		SPLIT_PART(SPLIT_PART(purchase_address, ',', 3), ' ', 2) AS state,
		SPLIT_PART(SPLIT_PART(purchase_address, ',', 3), ' ', 3) AS zip_code
    FROM cleaned;

    -- Truncate the cleaned table
    TRUNCATE TABLE cleaned;
END;
$$;

-- Stored procedure for data versioning
CREATE OR REPLACE PROCEDURE data_versioning()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD; --iterate all products
    existing_id VARCHAR;
BEGIN
	CREATE TABLE IF NOT EXISTS product (
	    product_id VARCHAR(255),
	    product_name VARCHAR(255) NOT NULL,
	    price_each NUMERIC(10, 2) NOT NULL,
	    last_update_date TIMESTAMP NOT NULL,
	    active_status CHAR(1) NOT NULL,
	    action_flag CHAR(1) NOT NULL,
		PRIMARY KEY (product_id, last_update_date)
		);

	
    FOR rec IN
        SELECT * FROM all_products ORDER BY order_date
    LOOP
        --check if is alr in product
        SELECT product_id INTO existing_id
        FROM product
        WHERE product_name = rec.product_name
          AND active_status = 'Y'; --check only active rec

        IF existing_id IS NULL THEN
            --insert if not in product table
            INSERT INTO product (
                product_id, product_name, price_each, last_update_date, active_status, action_flag
            )
            VALUES (
                'P' || nextval('product_id_sequence'),
                rec.product_name,
                rec.price_each,
                rec.order_date,
                'Y',
                'I'
            );
        ELSE
            --if prod exists, check if price_each is diff
            IF EXISTS (
                SELECT 1
                FROM product
                WHERE product_name = rec.product_name
                  AND price_each = rec.price_each
                  AND active_status = 'Y'
            ) THEN
                --skip if price is same
                CONTINUE;
            END IF;

            --if price is diff, change to inactive
            UPDATE product
            SET active_status = 'N'
            WHERE product_id = existing_id;

            --then insert the new version of the product w updated price
            INSERT INTO product (
                product_id, product_name, price_each, last_update_date, active_status, action_flag
            )
            VALUES (
                'P' || nextval('product_id_sequence'),
                rec.product_name,
                rec.price_each,
                rec.order_date,
                'Y',
                'U'
            );
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE product_dimension()
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS all_products (
	    product_name VARCHAR(255) NOT NULL,
	    price_each NUMERIC(10, 2) NOT NULL,
	    order_date TIMESTAMP NOT NULL
    );

    INSERT INTO all_products (product_name, price_each, order_date)
	(
		SELECT 
			product_name,
			price_each,
			order_date
		FROM cleaned --THIS IS FROM THE DATA NORMALIZATION AND CLEANSING 
	);

	CALL data_versioning();
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION change_price(target_product_id VARCHAR, new_price NUMERIC)
RETURNS VOID
LANGUAGE plpgsql
AS 
$$
BEGIN
	UPDATE product
	SET active_status = 'N'
	WHERE product_id = target_product_id;

	INSERT INTO product (
		product_id, product_name, new_price, last_update_date, active_status, action_flag
	) 
	
	SELECT product_id, product_name, new_price, NOW(), 'Y', 'U'
	  FROM product
	 WHERE product_id = target_product_id 
	 LIMIT 1;
END
$$;

CREATE SEQUENCE product_id_sequence
START 1
INCREMENT BY 1;

CREATE OR REPLACE PROCEDURE data_versioning()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD; --iterate all products
    existing_id VARCHAR;
BEGIN
	CREATE TABLE IF NOT EXISTS product (
	    product_id VARCHAR(255),
	    product_name VARCHAR(255) NOT NULL,
	    price_each NUMERIC(10, 2) NOT NULL,
	    last_update_date TIMESTAMP NOT NULL,
	    active_status CHAR(1) NOT NULL,
	    action_flag CHAR(1) NOT NULL,
		PRIMARY KEY (product_id, last_update_date)
		);

	
    FOR rec IN
        SELECT * FROM all_products ORDER BY order_date
    LOOP
        --check if is alr in product
        SELECT product_id INTO existing_id
        FROM product
        WHERE product_name = rec.product_name
          AND active_status = 'Y'; --check only active rec

        IF existing_id IS NULL THEN
            --insert if not in product table
            INSERT INTO product (
                product_id, product_name, price_each, last_update_date, active_status, action_flag
            )
            VALUES (
                'P' || nextval('product_id_sequence'),
                rec.product_name,
                rec.price_each,
                rec.order_date,
                'Y',
                'I'
            );
        ELSE
            --if prod exists, check if price_each is diff
            IF EXISTS (
                SELECT 1
                FROM product
                WHERE product_name = rec.product_name
                  AND price_each = rec.price_each
                  AND active_status = 'Y'
            ) THEN
                --skip if price is same
                CONTINUE;
            END IF;

            --if price is diff, change to inactive
            UPDATE product
            SET active_status = 'N'
            WHERE product_id = existing_id;

            --then insert the new version of the product w updated price
            INSERT INTO product (
                product_id, product_name, price_each, last_update_date, active_status, action_flag
            )
            VALUES (
                'P' || nextval('product_id_sequence'),
                rec.product_name,
                rec.price_each,
                rec.order_date,
                'Y',
                'U'
            );
        END IF;
    END LOOP;
END;
$$;
---------------------------------------------------------------------------------------------------



