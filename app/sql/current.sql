TRUNCATE TABLE landing_table;
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
CALL create_product_dimension();

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

----------------------------------------------------------------------------------------------------------------------

-- Procedure to handle data extraction from uploaded files
CREATE OR REPLACE PROCEDURE data_extraction(file_paths TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    file_path TEXT;
BEGIN
    RAISE NOTICE 'Starting data extraction...';

    -- Clear all relevant tables only on initial data extraction
    CALL clear_all_tables();

    -- Create landing_table if it doesn't exist
    CREATE TABLE IF NOT EXISTS landing_table (
        order_id VARCHAR(255),
        product VARCHAR(255),
        quantity_ordered VARCHAR(255),
        price_each VARCHAR(255),
        order_date VARCHAR(255),
        purchase_address VARCHAR(255)
    );

    -- Split the file paths into an array
    FOR file_path IN SELECT unnest(string_to_array(file_paths, ',')) LOOP
        -- Create a temporary table to hold the CSV data
        CREATE TEMP TABLE temp_csv (
            order_id VARCHAR(255),
            product VARCHAR(255),
            quantity_ordered VARCHAR(255),
            price_each VARCHAR(255),
            order_date VARCHAR(255),
            purchase_address VARCHAR(255)
        );

        -- Copy data from the CSV file into the temporary table
        EXECUTE format('COPY temp_csv FROM %L WITH CSV HEADER', file_path);

        -- Insert data from the temporary table into the landing_table
        INSERT INTO landing_table (order_id, product, quantity_ordered, price_each, order_date, purchase_address)
        SELECT order_id, product, quantity_ordered, price_each, order_date, purchase_address
        FROM temp_csv;

        -- Drop the temporary table
        DROP TABLE temp_csv;
    END LOOP;

	CALL call_all_procedures();

    RAISE NOTICE 'Data extraction completed.';
END;
$$;

-- Procedure to clear all relevant tables
CREATE OR REPLACE PROCEDURE clear_all_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'landing_table') THEN
        DELETE FROM landing_table;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cleaned') THEN
        DELETE FROM cleaned;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'for_cleaning') THEN
        DELETE FROM for_cleaning;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'invalid') THEN
        DELETE FROM invalid;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cleaned_normalized') THEN
        DELETE FROM cleaned_normalized;
    END IF;
    
    RAISE NOTICE 'All tables cleared.';
END;
$$;

-- Procedure to call all necessary procedures
CREATE OR REPLACE PROCEDURE call_all_procedures()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL data_mapping();
    CALL data_cleansing();
    CALL normalize_data();
    CALL create_product_dimension();
END;
$$;
---------------------------------------------------------------------------------------------------

-- Stored procedure for data mapping
CREATE OR REPLACE PROCEDURE data_mapping()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting data mapping...';

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
    
    RAISE NOTICE 'Tables created or verified.';

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
    
    RAISE NOTICE 'Invalid records inserted.';

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

    RAISE NOTICE 'Cleaned records inserted.';

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

    RAISE NOTICE 'Remaining records inserted into for_cleaning.';
END;
$$;

-- Stored procedure for data cleansing
CREATE OR REPLACE PROCEDURE data_cleansing()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting data cleansing...';

    -- Fix to 2 decimal points
    UPDATE for_cleaning
    SET price_each = TO_CHAR(ROUND(CAST(price_each AS NUMERIC), 2), 'FM999999999.00')
    WHERE price_each ~ '^[0-9]+(\.[0-9]{1,2})?$';

    RAISE NOTICE 'Price each fixed to 2 decimal points.';

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

    RAISE NOTICE 'One (1) Copy Complete duplicates inserted into cleaned.';

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

    RAISE NOTICE 'Other Complete duplicates inserted into invalid.';

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

    RAISE NOTICE 'Complete Duplicates deleted from for_cleaning.';

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

    RAISE NOTICE 'Non-duplicate records inserted into cleaned.';

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

    RAISE NOTICE 'Non-duplicate records deleted from for_cleaning.';

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

    RAISE NOTICE 'Duplicate IDs with different products or quantities handled.';

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

    RAISE NOTICE 'Updated records inserted into cleaned.';

    -- Trim data in the cleaned table
    UPDATE cleaned
    SET 
        product = TRIM(BOTH FROM product),
        purchase_address = TRIM(BOTH FROM purchase_address);

    RAISE NOTICE 'Data trimmed in cleaned table.';

    -- Clean up
    TRUNCATE for_cleaning;

    RAISE NOTICE 'Data cleansing completed.';
END;
$$;

-- Normalization logic
CREATE OR REPLACE PROCEDURE normalize_data()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting data normalization...';

    -- Create the cleaned_normalized table with appropriate data types
	DROP TABLE cleaned_normalized;
    CREATE TABLE IF NOT EXISTS cleaned_normalized (
        order_id INT, 
        product VARCHAR, 
        quantity_ordered INT, 
        price_each NUMERIC, 
        order_date TIMESTAMP, 
        month VARCHAR, 
        day VARCHAR, 
        year VARCHAR,
		halfyear VARCHAR,
		quarter VARCHAR, 
        street VARCHAR, 
        city VARCHAR, 
        state VARCHAR, 
        zip_code VARCHAR
    );

    RAISE NOTICE 'cleaned_normalized table created or verified.';

    -- Insert data into cleaned_normalized
    INSERT INTO cleaned_normalized 
        (order_id, product, quantity_ordered, price_each, order_date, month, day, year, halfyear, quarter, street, city, state, zip_code)
    SELECT 
        order_id, 
        product, 
        quantity_ordered, 
        price_each, 
        order_date, 
        to_char(EXTRACT(MONTH FROM order_date), 'FM00') AS month, 
        to_char(EXTRACT(DAY FROM order_date), 'FM00') AS day, 
        to_char(EXTRACT(YEAR FROM order_date), 'FM0000') AS year,
		to_char(CEIL(EXTRACT(MONTH FROM order_date)/6), 'FM00') as halfyear,
		to_char(EXTRACT(QUARTER FROM order_date), 'FM00') AS quarter,
        SPLIT_PART(purchase_address, ',', 1) AS street, 
        SPLIT_PART(purchase_address, ',', 2) AS city, 
        SPLIT_PART(SPLIT_PART(purchase_address, ',', 3), ' ', 2) AS state, 
        SPLIT_PART(SPLIT_PART(purchase_address, ',', 3), ' ', 3) AS zip_code
    FROM cleaned;

    RAISE NOTICE 'Data inserted into cleaned_normalized.';

    -- Truncate the cleaned table
    TRUNCATE TABLE cleaned;

    RAISE NOTICE 'cleaned table truncated.';

    RAISE NOTICE 'Data normalization completed.';
END;
$$;

-- Procedure to truncate all relevant tables
CREATE OR REPLACE PROCEDURE truncate_all_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM cleaned;
    DELETE FROM for_cleaning;
    DELETE FROM invalid;
    DELETE FROM cleaned_normalized;
END;
$$;

---------------------------------------------------------------------------------------------------
-- Stored procedure for data versioning (new)
CREATE OR REPLACE PROCEDURE populate_product_dimension()
LANGUAGE plpgsql
AS $$
DECLARE
	p_record RECORD;
	next_pk_id INTEGER := 1;
	next_pid_id INTEGER := 1;
BEGIN
    FOR p_record IN
    (
        SELECT *
        FROM product
    )
    LOOP
        --Checks if product exists
        IF NOT EXISTS
        (
            SELECT 1
            FROM product_dimension
            WHERE product_name = p_record.product
            AND active_status = 'Y'
        )
        THEN
            INSERT INTO product_dimension
            (
		product_key,
                product_id,
                product_name,
                price_each,
                last_update_date,
                active_status,
                action_flag
            )
            VALUES
            (
		'PK_' || LPAD(next_pk_id::TEXT, 4, '0'),
                'PID_' || LPAD(next_pid_id::TEXT, 4, '0'),
                p_record.product,
                p_record.price_each,
                p_record.order_date,
                'Y',
                'I'
            );

		next_pk_id := next_pk_id + 1;
		next_pid_id := next_pid_id + 1;

        ELSE
            IF EXISTS
            (
                SELECT 1
                FROM product_dimension
                WHERE product_name = p_record.product
                AND active_status = 'Y'
                AND price_each != p_record.price_each
            )
            THEN
                UPDATE product_dimension
                SET active_status = 'N'
                WHERE product_name = p_record.product
                AND active_status = 'Y';

                INSERT INTO product_dimension
                (
			product_key,
			product_id,
			product_name,
			price_each,
			last_update_date,
			active_status,
			action_flag
                )
                SELECT 
                    'PK_' || LPAD(next_pk_id::TEXT, 4, '0'),
                    pd.product_id,
                    p_record.product,
                    p_record.price_each,
                    p_record.order_date,
                    'Y',
                    'U'
                FROM product_dimension pd
                WHERE pd.product_name = p_record.product
                AND pd.active_status = 'N'
                LIMIT 1;

                next_pk_id := next_pk_id + 1;
            END IF;
        END IF;
    END LOOP;
END;
$$;
---------------------------------------------------------------------------------------------------------------
-- Stored procedure for product dimension(new)
CREATE OR REPLACE PROCEDURE create_product_dimension()
LANGUAGE plpgsql
AS $$
BEGIN
    --Safety measure ko ito
    DROP TABLE IF EXISTS product;

    CREATE TABLE product AS
    (
        SELECT DISTINCT
            product,
            price_each,
            MIN(order_date) AS order_date
        FROM cleaned_normalized
        GROUP BY product, price_each
        ORDER BY product, order_date
    );

    CREATE TABLE IF NOT EXISTS product_dimension
    (
	product_key VARCHAR(10),
        product_id VARCHAR(10),
        product_name VARCHAR(100),
        price_each DECIMAL(10,2),
        last_update_date TIMESTAMP,
        active_status CHAR(1),
        action_flag CHAR(1),
        PRIMARY KEY(product_key)
    );

    --handles the data versioning of the products
    CALL populate_product_dimension();
END;
$$;

---------------------------------------------------------------------------------------------------------------
--Function to change the product price (new)
/*Note: The function here will not work if product dimension doesn't exist yet so you will have to run the etl once in order to 
store this in the postgresql.*/
CREATE OR REPLACE FUNCTION handle_product_price_change()
RETURNS TRIGGER AS $$
DECLARE
    next_id INTEGER;
BEGIN
    IF (TG_OP = 'UPDATE') AND (OLD.active_status = 'Y') AND (OLD.price_each != NEW.price_each) THEN
        -- Get the next ID
        SELECT COALESCE(MAX(CAST(REPLACE(product_key, 'PK_', '') AS INTEGER)), 0) + 1 
        INTO next_id 
        FROM product_dimension;

        -- First deactivate the old record
        UPDATE product_dimension 
        SET active_status = 'N'
        WHERE product_key = OLD.product_key
        AND active_status = 'Y';

        -- Then insert new record
        INSERT INTO product_dimension 
        (
            product_key,
            product_id,
            product_name,
            price_each,
            last_update_date,
            active_status,
            action_flag
        ) VALUES 
        (
            'PK_' || LPAD(next_id::TEXT, 4, '0'),
            OLD.product_id,
            NEW.product_name,
            NEW.price_each,
            TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP, 'MM/DD/YY HH24:MI:SS'), 'MM/DD/YY HH24:MI:SS'),
            'Y',
            'U'
        );
        
        RETURN NULL;  -- This prevents the original UPDATE from happening
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger that only fires for price changes
CREATE TRIGGER tr_handle_product_price_change
BEFORE UPDATE OF price_each
ON product_dimension
FOR EACH ROW
EXECUTE FUNCTION handle_product_price_change();

-- Price change function remains the same
CREATE OR REPLACE FUNCTION change_product_price
(
    in p_product_name VARCHAR,
    in p_price_each DECIMAL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM product_dimension
        WHERE LOWER(product_name) = LOWER(TRIM(p_product_name))
        AND active_status = 'Y'
    ) THEN
        RAISE EXCEPTION 'Product % not found!', p_product_name;
    ELSE
        UPDATE product_dimension
        SET price_each = p_price_each
        WHERE LOWER(product_name) = LOWER(TRIM(p_product_name))
        AND active_status = 'Y';
    END IF;
END;
$$;

---------------------------------------------------------------------------------------------------------------
--Function to insert a new product
/*Note: The function here will not work if product dimension doesn't exist yet so you will have to run the etl once in order to 
store this in the postgresql.*/
CREATE OR REPLACE FUNCTION handle_product_insert()
RETURNS TRIGGER AS $$
DECLARE
    next_pk_id INTEGER;
    next_pid_id INTEGER;
BEGIN
    -- Get the next PK_ and PID_ numbers
    SELECT COALESCE(MAX(CAST(REPLACE(product_key, 'PK_', '') AS INTEGER)), 0) + 1 
    INTO next_pk_id 
    FROM product_dimension;
    
    SELECT COALESCE(MAX(CAST(REPLACE(product_id, 'PID_', '') AS INTEGER)), 0) + 1 
    INTO next_pid_id 
    FROM product_dimension;

    -- Set the values in NEW
    NEW.product_key := 'PK_' || LPAD(next_pk_id::TEXT, 4, '0');
    NEW.product_id := 'PID_' || LPAD(next_pid_id::TEXT, 4, '0');
    NEW.last_update_date := TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP, 'MM/DD/YY HH24:MI:SS'), 'MM/DD/YY HH24:MI:SS');
    NEW.active_status := 'Y';
    NEW.action_flag := 'I';
    
    RETURN NEW;  -- Allow the modified INSERT to proceed
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER tr_handle_product_insert
BEFORE INSERT
ON product_dimension
FOR EACH ROW
EXECUTE FUNCTION handle_product_insert();

-- Update the insert function to include all required fields
CREATE OR REPLACE FUNCTION insert_new_product
(
    in p_product_name VARCHAR,
    in p_price_each DECIMAL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if product already exists
    IF EXISTS (
        SELECT 1
        FROM product_dimension
        WHERE LOWER(product_name) = LOWER(TRIM(p_product_name))
        AND active_status = 'Y'
    ) THEN
        RAISE EXCEPTION 'Product % already exists!', p_product_name;
    ELSE
        INSERT INTO product_dimension 
        (
            product_name,
            price_each
        ) 
        VALUES 
        (
            INITCAP(TRIM(p_product_name)),
            p_price_each
        );
    END IF;
END;
$$;

---------------------------------------------------------------------------------------------------------------
-- Place code for creating time_dimension table here
	CREATE TABLE IF NOT EXISTS time_dimension 
	(
		time_id varchar,
		time_desc varchar,
		time_level int,
		parent_id varchar
	);

-- Place code for creating product_dimension table here
    CREATE TABLE IF NOT EXISTS product_dimension
    (
	product_key VARCHAR(10),
        product_id VARCHAR(10),
        product_name VARCHAR(100),
        price_each DECIMAL(10,2),
        last_update_date TIMESTAMP,
        active_status CHAR(1),
        action_flag CHAR(1),
        PRIMARY KEY(product_key)
    );

-- Place code for creating location_dimension table here

-- Place code for creating sales_fact table here



