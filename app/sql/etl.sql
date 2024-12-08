CREATE OR REPLACE PROCEDURE create_all_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS landing_table 
    (
        order_id VARCHAR(255),
        product VARCHAR(255),
        quantity_ordered VARCHAR(255),
        price_each VARCHAR(255),
        order_date VARCHAR(255),
        purchase_address VARCHAR(255)
    );

    CREATE TABLE IF NOT EXISTS cleaned 
    (
        order_id INT,
        product VARCHAR(255),
        quantity_ordered INT,
        price_each DECIMAL(10, 2),
        order_date TIMESTAMP,
        purchase_address VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS for_cleaning 
    (
        order_id VARCHAR(255),
        product VARCHAR(255),
        quantity_ordered VARCHAR(255),
        price_each VARCHAR(255),
        order_date VARCHAR(255),
        purchase_address VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS invalid 
    (
        order_id VARCHAR(255),
        product VARCHAR(255),
        quantity_ordered VARCHAR(255),
        price_each VARCHAR(255),
        order_date VARCHAR(255),
        purchase_address VARCHAR(255)
	);

	CREATE TABLE IF NOT EXISTS time_dimension 
	(
		time_id varchar,
		time_desc varchar,
		time_level int,
		parent_id varchar
	);

-- product_dimension table
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

-- location_dimension table
	CREATE TABLE IF NOT EXISTS location_dimension 
    (
	    location_id VARCHAR(50) PRIMARY KEY, 
	    location_name VARCHAR(255),          
	    level INT,                           
	    parent_id VARCHAR(50)               
	);

-- final_fact table
    CREATE TABLE IF NOT EXISTS final_fact 
    (
        order_id INT,
        product_id VARCHAR,
        location_id VARCHAR, 
        time_id VARCHAR, 
        quantity_ordered INT,
        total_sales NUMERIC
    );
END;
$$;

Call create_all_tables();

CREATE OR REPLACE FUNCTION log_message(message TEXT) RETURNS VOID 
LANGUAGE plpgsql
AS $$
    BEGIN
        PERFORM pg_notify('log_channel', message);
    END;
$$;

---------------------------------------------------------------------------------------------------------------
-- [Start of the ETL process]

-- Procedure to handle data extraction from uploaded files
CREATE OR REPLACE PROCEDURE data_extraction(file_paths TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    file_path TEXT;
BEGIN
    PERFORM log_message('Starting data extraction...');

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

    PERFORM log_message('Data extraction completed.');
END;
$$;

-- Procedure to clear all relevant tables
CREATE OR REPLACE PROCEDURE clear_all_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM landing_table;
    DELETE FROM cleaned;
    DELETE FROM for_cleaning;
    DELETE FROM invalid;
    DELETE FROM cleaned_normalized;
    DELETE FROM product_dimension;
    DELETE FROM time_dimension;
    DELETE FROM location_dimension;
    DELETE FROM final_fact;
    DELETE FROM data_cube;
    DELETE FROM sliced_cube;

    PERFORM log_message('All tables cleared.');
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
    CALL create_location_dimension();
    CALL create_time_dimension();
    CALL create_final_fact_table();
    CALL create_data_cube();
END;
$$;
----------------------------------------------------------------------------------------------------------------------
-- Stored procedure for data mapping
CREATE OR REPLACE PROCEDURE data_mapping()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM log_message('Starting data mapping...');

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
    
    PERFORM log_message('Tables created or verified.');

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
    
    PERFORM log_message('Invalid records inserted.');

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

    PERFORM log_message('Cleaned records inserted.');

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

    PERFORM log_message('Remaining records inserted into for_cleaning.');
END;
$$;

-- Stored procedure for data cleansing
CREATE OR REPLACE PROCEDURE data_cleansing()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM log_message('Starting data cleansing...');

    -- Fix to 2 decimal points
    UPDATE for_cleaning
    SET price_each = TO_CHAR(ROUND(CAST(price_each AS NUMERIC), 2), 'FM999999999.00')
    WHERE price_each ~ '^[0-9]+(\.[0-9]{1,2})?$';

    PERFORM log_message('Price each fixed to 2 decimal points.');

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

    PERFORM log_message('One (1) Copy Complete duplicates inserted into cleaned.');

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

    PERFORM log_message('Other Complete duplicates inserted into invalid.');

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

    PERFORM log_message('Complete Duplicates deleted from for_cleaning.');

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

    PERFORM log_message('Non-duplicate records inserted into cleaned.');

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

    PERFORM log_message('Non-duplicate records deleted from for_cleaning.');

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

    PERFORM log_message('Duplicate IDs with different products or quantities handled.');

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

    PERFORM log_message('Updated records inserted into cleaned.');

    -- Trim data in the cleaned table
    UPDATE cleaned
    SET 
        product = TRIM(BOTH FROM product),
        purchase_address = TRIM(BOTH FROM purchase_address);

    PERFORM log_message('Data trimmed in cleaned table.');

    -- Clean up
    TRUNCATE for_cleaning;

    PERFORM log_message('Data cleansing completed.');
END;
$$;

-- Normalization logic
CREATE OR REPLACE PROCEDURE normalize_data()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM log_message('Starting data normalization...');

    -- Create the cleaned_normalized table with appropriate data types
	DROP TABLE IF EXISTS cleaned_normalized;
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

    PERFORM log_message('cleaned_normalized table created or verified.');

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

    PERFORM log_message('Data inserted into cleaned_normalized.');

    -- Truncate the cleaned table
    TRUNCATE TABLE cleaned;

    PERFORM log_message('cleaned table truncated.');

    PERFORM log_message('Data normalization completed.');
END;
$$;
---------------------------------------------------------------------------------------------------------------
-- [Product Dimension]

-- Stored procedure for data versioning
CREATE OR REPLACE PROCEDURE populate_product_dimension()
LANGUAGE plpgsql
AS $$
DECLARE
	p_record RECORD;
	next_pk_id INTEGER := 1;
	next_pid_id INTEGER := 1;
BEGIN
    TRUNCATE TABLE product_dimension;

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

-- Stored procedure for product dimension
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

--Function to change the product price
/*Note: The function here will not work if product dimension doesn't exist yet so you will have to run the etl once in order to store this in the postgresql.*/
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
DROP TRIGGER IF EXISTS tr_handle_product_price_change ON product_dimension;
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

--Function to insert a new product
CREATE OR REPLACE FUNCTION insert_new_product(
    in p_product_name VARCHAR,
    in p_price_each DECIMAL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    next_pk_id INTEGER;
    next_pid_id INTEGER;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM product_dimension
        WHERE LOWER(product_name) =LOWER(TRIM(p_product_name))
        AND active_status = 'Y'
    ) THEN
        RAISE EXCEPTION 'Product % already exists!', p_product_name;
    ELSE
        -- Get the next IDs
        SELECT COALESCE(MAX(CAST(REPLACE(product_key, 'PK_', '') AS INTEGER)), 0) + 1 
        INTO next_pk_id 
        FROM product_dimension;
        
        SELECT COALESCE(MAX(CAST(REPLACE(product_id, 'PID_', '') AS INTEGER)), 0) + 1 
        INTO next_pid_id 
        FROM product_dimension;

        -- Insert new product
        INSERT INTO product_dimension (
            product_key,
            product_id,
            product_name,
            price_each,
            last_update_date,
            active_status,
            action_flag
        ) VALUES (
            'PK_' || LPAD(next_pk_id::TEXT, 4, '0'),
            'PID_' || LPAD(next_pid_id::TEXT, 4, '0'),
            INITCAP(TRIM(p_product_name)),
            p_price_each,
            TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP, 'MM/DD/YY HH24:MI:SS'), 'MM/DD/YY HH24:MI:SS'),
            'Y',
            'I'
        );
    END IF;
END;
$$;


---------------------------------------------------------------------------------------------------------------
-- [Time Dimension]

-- Procedure to populate time dimension
CREATE OR REPLACE PROCEDURE populate_time_dimension() 
AS $$	
BEGIN
	-- year
	INSERT INTO time_dimension(time_id, time_desc, time_level, parent_id) 
		SELECT DISTINCT('Y'||year) AS time_id, year, 4, NULL
		  FROM cleaned_normalized ORDER BY time_id;

	-- halfyear 
	INSERT INTO time_dimension(time_id, time_desc, time_level, parent_id) 
		SELECT DISTINCT('HY'||year||halfyear) AS time_id, year||', half '||halfyear, 3, ('Y'||year)  
			FROM cleaned_normalized ORDER BY time_id;

	-- quarter
	INSERT INTO time_dimension(time_id, time_desc, time_level, parent_id) 
		SELECT DISTINCT('Q'||year||halfyear||quarter) AS time_id, year||', half '||halfyear||', quarter '||quarter, 2, ('HY'||year||halfyear)
			FROM cleaned_normalized ORDER BY time_id;

	-- month
	INSERT INTO time_dimension(time_id, time_desc, time_level, parent_id) 
		SELECT DISTINCT('MO'||year||halfyear||quarter||month) AS time_id, year||', half '||halfyear||', quarter '||quarter||', month '||month, 1, ('Q'||year||halfyear||quarter)
			FROM cleaned_normalized ORDER BY time_id;

	-- day 
	INSERT INTO time_dimension(time_id, time_desc, time_level, parent_id) 
		SELECT DISTINCT('D'||year||halfyear||quarter||month||day) AS time_id, year||', half '||halfyear||', quarter '||quarter||', month '||month||', day '||day, 0, ('MO'||year||halfyear||quarter||month)
			FROM cleaned_normalized ORDER BY time_id;
END;
$$ LANGUAGE plpgsql;

--Procedure to create the time dimension
CREATE OR REPLACE PROCEDURE create_time_dimension()
AS $$
BEGIN
    DROP TABLE time_dimension;
    CREATE TABLE IF NOT EXISTS time_dimension 
	(
		time_id varchar,
		time_desc varchar,
		time_level int,
		parent_id varchar
	);

    CALL populate_time_dimension();
END;
$$ LANGUAGE plpgsql;

-- Procedure to handle time insert
CREATE OR REPLACE FUNCTION handle_time_insert()
RETURNS TRIGGER AS $$
DECLARE
    v_year VARCHAR;
    v_halfyear VARCHAR;
    v_quarter VARCHAR;
    v_month VARCHAR;
    v_day VARCHAR;
BEGIN
    -- Extract time components from NEW.order_date
    v_year := to_char(NEW.order_date, 'YYYY');
    v_halfyear := to_char(CEIL(EXTRACT(MONTH FROM NEW.order_date) / 6), 'FM00');
    v_quarter := to_char(EXTRACT(QUARTER FROM NEW.order_date), 'FM00');
    v_month := to_char(EXTRACT(MONTH FROM NEW.order_date), 'FM00');
    v_day := to_char(EXTRACT(DAY FROM NEW.order_date), 'FM00');

    -- Insert year if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'Y' || v_year) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('Y' || v_year, v_year, 4, NULL);
    END IF;

    -- Insert halfyear if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'H' || v_year || v_halfyear) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('H' || v_year || v_halfyear, v_year || ' H' || v_halfyear, 3, 'Y' || v_year);
    END IF;

    -- Insert quarter if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'Q' || v_year || v_quarter) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('Q' || v_year || v_quarter, v_year || ' Q' || v_quarter, 2, 'Y' || v_year);
    END IF;

    -- Insert month if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'M' || v_year || v_month) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('M' || v_year || v_month, v_year || '-' || v_month, 1, 'Y' || v_year);
    END IF;

    -- Insert day if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'D' || v_year || v_month || v_day) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('D' || v_year || v_month || v_day, v_year || '-' || v_month || '-' || v_day, 0, 'M' || v_year || v_month);
    END IF;

    RETURN NEW;  -- Allow the modified INSERT to proceed
END;
$$ LANGUAGE plpgsql;

-- Create the trigger that only fires for time inserts
DROP TRIGGER IF EXISTS tr_handle_time_insert ON time_dimension;
CREATE TRIGGER tr_handle_time_insert
BEFORE INSERT
ON time_dimension
FOR EACH ROW
EXECUTE FUNCTION handle_time_insert();

-- Update the insert function to include all required fields
CREATE OR REPLACE FUNCTION insert_new_time
(
    in p_order_date TIMESTAMP
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_year VARCHAR;
    v_halfyear VARCHAR;
    v_quarter VARCHAR;
    v_month VARCHAR;
    v_day VARCHAR;
BEGIN
    -- Extract time components
    v_year := to_char(p_order_date, 'YYYY');
    v_halfyear := to_char(CEIL(EXTRACT(MONTH FROM p_order_date) / 6), 'FM00');
    v_quarter := to_char(EXTRACT(QUARTER FROM p_order_date), 'FM00');
    v_month := to_char(EXTRACT(MONTH FROM p_order_date), 'FM00');
    v_day := to_char(EXTRACT(DAY FROM p_order_date), 'FM00');

    -- Insert year if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'Y' || v_year) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('Y' || v_year, v_year, 4, NULL);
    END IF;

    -- Insert halfyear if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'H' || v_year || v_halfyear) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('H' || v_year || v_halfyear, v_year || ' H' || v_halfyear, 3, 'Y' || v_year);
    END IF;

    -- Insert quarter if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'Q' || v_year || v_quarter) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('Q' || v_year || v_quarter, v_year || ' Q' || v_quarter, 2, 'Y' || v_year);
    END IF;

    -- Insert month if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'M' || v_year || v_month) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('M' || v_year || v_month, v_year || '-' || v_month, 1, 'Y' || v_year);
    END IF;

    -- Insert day if not exists
    IF NOT EXISTS (SELECT 1 FROM time_dimension WHERE time_id = 'D' || v_year || v_month || v_day) THEN
        INSERT INTO time_dimension (time_id, time_desc, time_level, parent_id)
        VALUES ('D' || v_year || v_month || v_day, v_year || '-' || v_month || '-' || v_day, 0, 'M' || v_year || v_month);
    END IF;
END;
$$;

---------------------------------------------------------------------------------------------------------------
-- [Location Dimension]

-- Procedure to populate location dimension
CREATE OR REPLACE PROCEDURE populate_location_dimension()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    city_count INT;
    street_count INT;
BEGIN
    TRUNCATE TABLE location_dimension;
	
    INSERT INTO location_dimension (location_id, location_name, level, parent_id)
    SELECT DISTINCT 
        UPPER(state) AS location_id,
        state AS location_name,
        3 AS level,
        NULL AS parent_id
    FROM cleaned_normalized;

	INSERT INTO location_dimension (location_id, location_name, level, parent_id)
    SELECT DISTINCT 
        UPPER(state)||zip_code AS location_id,
        state || ' ' || zip_code AS location_name,
        2 AS level,
        UPPER(state) AS parent_id
    FROM cleaned_normalized;

	INSERT INTO location_dimension (location_id, location_name, level, parent_id)
	SELECT 
	    UPPER(cn.state) || cn.zip_code || 'C' || city_rank.city_number AS location_id,
	    cn.city || ', ' || cn.state || ' ' || cn.zip_code AS location_name,
	    1 AS level,
	    UPPER(cn.state) || zip_code AS parent_id
	FROM 
	    (SELECT DISTINCT state, zip_code, city
	     FROM cleaned_normalized) AS cn
	LEFT JOIN (
	    SELECT 
	        state, 
	        city, 
	        ROW_NUMBER() OVER (PARTITION BY state ORDER BY city) AS city_number
	    FROM 
	        (SELECT DISTINCT state, city FROM cleaned_normalized) AS cities
	) AS city_rank
	ON cn.state = city_rank.state AND cn.city = city_rank.city
	ORDER BY 
	    cn.state, cn.city;

	INSERT INTO location_dimension (location_id, location_name, level, parent_id)
	SELECT
		UPPER(cn.state) || cn.zip_code || 'C' || city_rank.city_number || 'S' || street_rank.street_number  AS location_id,
	    cn.street || ', ' || cn.city || ', ' || cn.state || ' ' || cn.zip_code AS location_name,
	    0 AS level, 
	    UPPER(cn.state) || cn.zip_code || 'C' || city_rank.city_number AS parent_id
	FROM 
	    (SELECT DISTINCT state, zip_code, city, street
	     FROM cleaned_normalized) AS cn
	LEFT JOIN (
	    SELECT 
	        state, 
	        city, 
	        ROW_NUMBER() OVER (PARTITION BY state ORDER BY city) AS city_number
	    FROM 
	        (SELECT DISTINCT state, city FROM cleaned_normalized) AS cities
	) AS city_rank
	ON cn.state = city_rank.state AND cn.city = city_rank.city
	LEFT JOIN (
	    SELECT 
	        state, 
	        city, 
	        street,
	        ROW_NUMBER() OVER (PARTITION BY state, city ORDER BY street) AS street_number
	    FROM 
	        (SELECT DISTINCT state, city, street FROM cleaned_normalized) AS streets
	) AS street_rank
	ON cn.state = street_rank.state AND cn.city = street_rank.city AND cn.street = street_rank.street
	ORDER BY cn.state, cn.city, cn.street;

END;
$$;

-- Procedure to create the location dimension
CREATE OR REPLACE PROCEDURE create_location_dimension()
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS location_dimension;
	CREATE TABLE IF NOT EXISTS location_dimension (
	    location_id VARCHAR(50) PRIMARY KEY, 
	    location_name VARCHAR(255),          
	    level INT,                           
	    parent_id VARCHAR(50)               
	);

	CALL populate_location_dimension();
END;
$$;

---------------------------------------------------------------------------------------------------------------
-- [Final Fact Table]

CREATE OR REPLACE PROCEDURE create_final_fact_table()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop the final_fact table if it exists
    DROP TABLE IF EXISTS final_fact;

    -- Create the final_fact table
    CREATE TABLE IF NOT EXISTS final_fact (
        order_id INT,
        product_id VARCHAR,
        location_id VARCHAR, 
        time_id VARCHAR, 
        quantity_ordered INT,
        total_sales NUMERIC
    );

    -- Insert data into the final_fact table
    INSERT INTO final_fact (order_id, product_id, quantity_ordered, location_id, time_id, total_sales)
    SELECT
        cn.order_id,
        pd.product_id,
        cn.quantity_ordered,
        ld.location_id,
        ('D' || cn.year || cn.halfyear || cn.quarter || cn.month || cn.day) AS time_id,
        cn.quantity_ordered * (
            SELECT sub_pd.price_each
            FROM product_dimension sub_pd
            WHERE sub_pd.product_id = pd.product_id
              AND sub_pd.last_update_date <= cn.order_date
            ORDER BY sub_pd.last_update_date DESC
            LIMIT 1
        ) AS total_sales
    FROM cleaned_normalized cn
    INNER JOIN product_dimension pd
        ON TRIM(LOWER(cn.product)) = TRIM(LOWER(pd.product_name))
    INNER JOIN location_dimension ld
        ON TRIM(LOWER(ld.location_name)) = TRIM(LOWER(
            cn.street || ', ' || cn.city || ', ' || cn.state || ' ' || cn.zip_code
        ));

    PERFORM log_message('final_fact table created and populated.');
END;
$$;


CREATE OR REPLACE PROCEDURE create_data_cube()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop the data_cube table if it exists
    DROP TABLE IF EXISTS data_cube;

    -- Create the data_cube table with aggregated data
    CREATE TABLE data_cube AS
    SELECT
        product_id,
        time_id,
        location_id,
        SUM(total_sales) AS total_sales_sum
    FROM final_fact
    GROUP BY CUBE(product_id, time_id, location_id)
	ORDER BY 
		product_id NULLS FIRST,
		time_id NULLS FIRST,
		location_id NULLS FIRST;

    PERFORM log_message('data_cube table created and populated.');
END;
$$;

---------------------------------------------------------------------------------------------------------------
-- FOR SLICING
---------------------------------------------------------------------------------------------------------------
-- [Extract Grains Location]
-- Procedure to extract the grains of the location based on 2 parameters:
-- extract_grains_loc(Grain level, Highest Parent_ID)
DROP PROCEDURE IF EXISTS extract_grains_loc(integer, varchar);
CREATE OR REPLACE PROCEDURE public.extract_grains_loc(
	IN grain integer,
	IN top_node varchar)
LANGUAGE 'plpgsql'
AS $$

DECLARE
	top_level INT := (SELECT level FROM location_dimension WHERE location_id = top_node);
	counter INT := 4;
BEGIN
	RAISE NOTICE 'top_level is %', top_level;
	DROP TABLE IF EXISTS locationResult;
	CREATE TEMP TABLE locationResult AS
    SELECT * FROM location_dimension WHERE 1=2;

	-- If top node is null, inserts the grains year
	IF top_node IS NULL THEN
		INSERT INTO locationResult
		SELECT * FROM location_dimension ld WHERE ld.level = 3;

	-- If grain is null, inserts all the records under the top_node 
	ELSIF top_node IS NOT NULL AND grain IS NULL THEN
		IF counter = 4 THEN
			INSERT INTO locationResult
			SELECT * FROM location_dimension WHERE parent_id = top_node;
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 3 THEN
			INSERT INTO locationResult
			SELECT * FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id = top_node);
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 2 THEN
			INSERT INTO locationResult
			SELECT * FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id = top_node));
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 1 THEN
			INSERT INTO locationResult
			SELECT * FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id IN (SELECT location_id FROM location_dimension WHERE parent_id = top_node)));
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
	
	-- Catch invalid parameter inputs
	ELSIF grain > top_level THEN
		RAISE NOTICE 'GRAIN IS BIGGER THAN TOP_NODE';

	-- If grain = top_level, insert the record itself
	ELSIF grain = top_level THEN
		INSERT INTO locationResult
        SELECT * FROM location_dimension WHERE location_id = top_node;
		RAISE NOTICE 'GRAIN IS EQUAL TO TOP_NODE';

	-- If grain is lower than top_level, then properly insert the requested grain underneath the top_node
	ELSE
		IF (top_level - grain) = 1 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 1';
			INSERT INTO locationResult
			SELECT t1.* FROM location_dimension t1 WHERE t1.parent_id = top_node;
			
		ELSIF (top_level - grain) = 2 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 2'; 
			INSERT INTO locationResult
			SELECT t1.* FROM location_dimension t1 WHERE t1.parent_id IN (SELECT t2.location_id FROM location_dimension t2 WHERE t2.parent_id = top_node);
			
		ELSIF (top_level - grain) = 3 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 3';
			INSERT INTO locationResult
			SELECT t1.* FROM location_dimension t1 WHERE t1.parent_id IN (SELECT t2.location_id FROM location_dimension t2 WHERE t2.parent_id IN (SELECT t3.location_id FROM location_dimension t3 WHERE t3.parent_id = top_node ));
		ELSE
			RAISE NOTICE 'RUN SUCCESSFULLY IN 4';
			INSERT INTO locationResult
			SELECT t1.* FROM location_dimension t1 WHERE t1.parent_id IN (SELECT t2.location_id FROM location_dimension t2 WHERE t2.parent_id IN (SELECT t3.location_id FROM location_dimension t3 WHERE t3.parent_id IN (SELECT t4.location_id FROM location_dimension t4 WHERE t4.parent_id = top_node)));
		END IF;
	END IF;
END;
$$;
---------------------------------------------------------------------------------------------------------------
-- [EXTRACT GRAINS TIME]
-- Procedure to extract the grains of time based on 2 parameters:
-- extract_grains_time(Grain level, Highest Parent_ID)
CREATE OR REPLACE PROCEDURE public.extract_grains_time(
	IN grain integer,
	IN top_node text)
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
	top_level INT := (SELECT time_level FROM time_dimension WHERE time_id = top_node);
	counter INT := 4;

BEGIN
	RAISE NOTICE 'STARTING TOP_LEVEL IS %', top_level;
	DROP TABLE IF EXISTS timeResult;
	CREATE TEMP TABLE timeResult AS
    SELECT * FROM time_dimension WHERE 1=2;

	-- If top node is null, inserts the grains year
	IF top_node IS NULL AND grain IS NOT NULL THEN
		INSERT INTO timeResult
		SELECT * FROM time_dimension WHERE time_level = 4;

	-- If grain is null, inserts all the records under the top_node 
	ELSIF top_node IS NOT NULL AND grain IS NULL THEN
		IF counter = 4 THEN
			INSERT INTO timeResult
			SELECT * FROM time_dimension WHERE  parent_id = top_node;
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 3 THEN
			INSERT INTO timeResult
			SELECT * FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id = top_node);
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 2 THEN
			INSERT INTO timeResult
			SELECT * FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id = top_node));
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;
		IF counter = 1 THEN
			INSERT INTO timeResult
			SELECT * FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id IN (SELECT time_id FROM time_dimension WHERE parent_id = top_node)));
			counter = counter - 1;
			RAISE NOTICE 'COUNTER IS NOW % AFTER BEING SUBTRACTED', counter;
		END IF;

	-- Catch invalid parameter inputs
	ELSIF grain > top_level THEN
		RAISE NOTICE 'GRAIN IS BIGGER THAN TOP_NODE';

	-- If grain = top_level, insert the record itself
	ELSIF grain = top_level THEN
		INSERT INTO timeResult
        SELECT * FROM time_dimension WHERE time_id = top_node;
		RAISE NOTICE 'GRAIN IS EQUAL TO TOP_NODE';

	-- If grain is lower than top_level, then properly insert the requested grain underneath the top_node
	ELSE
		IF (top_level - grain) = 1 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 1';
			INSERT INTO timeResult
			SELECT t1.* FROM time_dimension t1 WHERE t1.parent_id = top_node;
			
		ELSIF (top_level - grain) = 2 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 2'; 
			INSERT INTO timeResult
			SELECT t1.* FROM time_dimension t1 WHERE t1.parent_id IN (SELECT t2.time_id FROM time_dimension t2 WHERE t2.parent_id = top_node);
			
		ELSIF (top_level - grain) = 3 THEN
			RAISE NOTICE 'RUN SUCCESSFULLY IN 3';
			INSERT INTO timeResult
			SELECT t1.* FROM time_dimension t1 WHERE t1.parent_id IN (SELECT t2.time_id FROM time_dimension t2 WHERE t2.parent_id IN (SELECT t3.time_id FROM time_dimension t3 WHERE t3.parent_id = top_node ));
		ELSE
			RAISE NOTICE 'RUN SUCCESSFULLY IN 4';
			INSERT INTO timeResult
			SELECT t1.* FROM time_dimension t1 WHERE t1.parent_id IN (SELECT t2.time_id FROM time_dimension t2 WHERE t2.parent_id IN (SELECT t3.time_id FROM time_dimension t3 WHERE t3.parent_id IN (SELECT t4.time_id FROM time_dimension t4 WHERE t4.parent_id = top_node)));
		END IF;
	END IF;
END;
$BODY$;
---------------------------------------------------------------------------------------------------------------
--[Slice Cube]
-- Procedure to slice the data cube based on 2 parameters for time and location
-- specific products can be found using a where clause instead
-- Procedure works by taking in a top parent time or a top parent location, these parameters may be null
-- Output gets inserted into sliced_cube table
CREATE OR REPLACE PROCEDURE slice_cube(IN top_node_time text, IN top_node_loc text)
	LANGUAGE 'plpgsql'
	AS $$
	BEGIN
	DROP TABLE IF EXISTS sliced_cube;
	CREATE TABLE IF NOT EXISTS sliced_cube(
			product_id varchar,
			time_id varchar,
			location_id varchar,
			total_sales_sum numeric
			);
	
		IF top_node_time IS NULL AND top_node_loc IS NULL THEN
			INSERT INTO sliced_cube SELECT * FROM data_cube;
			
		ELSIF (top_node_time IS NOT NULL AND top_node_loc IS NULL) THEN
			CALL extract_grains_time(0, top_node_time);
			INSERT INTO sliced_cube
				SELECT
					product_id,
					time_id,
					location_id,
					SUM(total_sales) as total_sales_sum
				FROM final_fact
				GROUP BY
					CUBE(product_id, time_id, location_id)
				HAVING time_id IN (SELECT time_id FROM timeResult);
		ELSIF (top_node_time IS NULL AND top_node_loc IS NOT NULL) THEN
			CALL extract_grains_loc(0, top_node_loc);
			INSERT INTO sliced_cube
				SELECT
					product_id,
					time_id,
					location_id,
					SUM(total_sales) as total_sales_sum
				FROM final_fact
				GROUP BY
					CUBE(product_id, time_id, location_id)
				HAVING location_id IN (SELECT location_id FROM locationResult);
		ELSE
			CALL extract_grains_time(NULL, top_node_time);
			CALL extract_grains_loc(NULL, top_node_loc);
			INSERT INTO sliced_cube
				SELECT
					product_id,
					time_id,
					location_id,
					SUM(total_sales) as total_sales_sum
				FROM final_fact
				GROUP BY
					CUBE(product_id, time_id, location_id)
				HAVING time_id IN (SELECT time_id FROM timeResult) AND
					   location_id IN (SELECT location_id FROM locationResult);
		END IF;
	END;
	$$;