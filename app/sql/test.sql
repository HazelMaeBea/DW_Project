-- [For Testing Purposes]

-- Truncate all tables
    TRUNCATE TABLE landing_table;
    TRUNCATE TABLE cleaned;
    TRUNCATE TABLE for_cleaning;
    TRUNCATE TABLE invalid;
    TRUNCATE TABLE cleaned_normalized;
    TRUNCATE TABLE product_dimension;
    TRUNCATE TABLE time_dimension;
    TRUNCATE TABLE location_dimension;
    TRUNCATE TABLE final_fact;
    TRUNCATE TABLE data_cube;

-- Select statements
    SELECT * FROM landing_table;
    SELECT * FROM cleaned;
    SELECT * FROM for_cleaning;
    SELECT * FROM invalid;
    SELECT * FROM cleaned_normalized;
    SELECT * FROM product_dimension;
    SELECT * FROM time_dimension ORDER BY time_level DESC;
    SELECT * FROM location_dimension ORDER BY level DESC;
    SELECT * FROM final_fact;
    SELECT * FROM data_cube;

-- Call the procedures
    CALL truncate_all_tables();
    CALL data_extraction(); -- just to complete the list of procedures
    CALL data_mapping();
    CALL data_cleansing();
    CALL normalize_data();

    CALL create_product_dimension();
    CALL populate_product_dimension();
    CALL create_time_dimension();
    CALL create_location_dimension();
    CALL populate_location_dimension();
    CALL create_final_fact_table();
    CALL create_data_cube();

-- Testing for duplicates, both complete and only ids
    SELECT * FROM for_cleaning
    WHERE order_id = '150925';

-- Checking for duplicates in cleaned table
    SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, purchase_address,
            CASE WHEN COUNT(*) > 1 THEN 'T'
            ELSE 'F' END
    FROM cleaned
    GROUP BY order_id, product, quantity_ordered, price_each, order_date, purchase_address
    -- HAVING COUNT(*) > 1
    ORDER BY order_id, product;

-- Checking for duplicates in cleaned_normalized table
    SELECT DISTINCT(order_id), product, quantity_ordered, price_each, order_date, street, city, state, zip_code,
            CASE WHEN COUNT(*) > 1 THEN 'T'
            ELSE 'F' END
    FROM cleaned_normalized
    GROUP BY order_id, product, quantity_ordered, price_each, order_date, street, city, state, zip_code
    -- HAVING COUNT(*) > 1
    ORDER BY order_id, product;

-- Tables used for extraction
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

-- Procedure to truncate all relevant tables (for testing purposes)
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
