-- SEQUENCE for automatically add num for FACT_SALE_ID
DROP SEQUENCE FACT_SALE_ID;
CREATE SEQUENCE FACT_SALE_ID MINVALUE 1 START WITH 1;     


-- Create a cursor for receiving the data
DECLARE
    CURSOR c_DS IS SELECT * FROM DATASTREAM ORDER BY DATASTREAM_ID;    
    
    TYPE tuples_DS IS TABLE OF c_DS%ROWTYPE;  
    v_DS tuples_DS;
    
    c_DS_MD SYS_REFCURSOR;     
    
    TYPE fact_sale_tuples IS RECORD (     
        PRODUCT_ID VARCHAR2(6),
        CUSTOMER_ID VARCHAR2(4),
        CUSTOMER_NAME VARCHAR2(28),
        WAREHOUSE_ID VARCHAR2(4),
        WAREHOUSE_NAME VARCHAR2(20),
        T_DATE DATE,
        QUANTITY_SOLD NUMBER(3),
        PRODUCT_NAME VARCHAR2(28),
        SUPPLIER_ID VARCHAR2(5),
        SUPPLIER_NAME VARCHAR2(30),
        SALE_PRICE NUMBER(5,2),
        TOTAL_SALE NUMBER(8,2)
    );
    fsale fact_sale_tuples;
    
	-- RECORD
    rec INTEGER;
    
    datastream_count INTEGER;       
    
BEGIN
    OPEN c_DS;
	-- Get number of tuples from DATASTREAM and repeat the following step untill all data load to DW
    SELECT COUNT(*) INTO datastream_count FROM DATASTREAM;      
    FOR loop in 1..(datastream_count/100) LOOP      
    
		-- Read 100 tuples from DATASTREAM 
        FETCH c_DS BULK COLLECT INTO v_DS LIMIT 100;   
        
        FOR i IN 1..100 LOOP    
            
			-- Retrieve relevant tuple from MATSTERDATA using PRODUCT_ID as index and DATASTREAM using DATASTREAM_ID
            OPEN c_DS_MD FOR
                SELECT A.PRODUCT_ID,A.CUSTOMER_ID,A.CUSTOMER_NAME,A.WAREHOUSE_ID,A.WAREHOUSE_NAME,A.T_DATE,A.QUANTITY_SOLD,B.PRODUCT_NAME,
                        B.SUPPLIER_ID,B.SUPPLIER_NAME,B.SALE_PRICE,(A.QUANTITY_SOLD * B.SALE_PRICE) AS "TOTAL_SALE"
                FROM DATASTREAM A, MASTERDATA B WHERE A.DATASTREAM_ID = v_DS(i).DATASTREAM_ID AND B.PRODUCT_ID = v_DS(i).PRODUCT_ID;  
           
            FETCH c_DS_MD INTO fsale;   
            
			-- DIM_CUSTOMERS
            SELECT COUNT(*) INTO rec FROM DIM_CUSTOMERS WHERE DIM_CUSTOMERS.CUSTOMER_ID = fsale.CUSTOMER_ID;
			IF rec=0 THEN
                INSERT INTO DIM_CUSTOMERS VALUES(fsale.CUSTOMER_ID,fsale.CUSTOMER_NAME);
            END IF;
			
			-- DIM_WAREHOUSE
            SELECT COUNT(*) INTO rec FROM DIM_WAREHOUSE WHERE DIM_WAREHOUSE.WAREHOUSE_ID = fsale.WAREHOUSE_ID;
            IF rec=0 THEN
                INSERT INTO DIM_WAREHOUSE VALUES(fsale.WAREHOUSE_ID,fsale.WAREHOUSE_NAME);
            END IF;

			-- DIM_PRODUCTS
            SELECT COUNT(*) INTO rec FROM DIM_PRODUCTS WHERE DIM_PRODUCTS.PRODUCT_ID = fsale.PRODUCT_ID;
            IF rec=0 THEN
                INSERT INTO DIM_PRODUCTS VALUES(fsale.PRODUCT_ID,fsale.PRODUCT_NAME);
            END IF;			
			
			-- DIM_SUPPLIERS
            SELECT COUNT(*) INTO rec FROM DIM_SUPPLIERS WHERE DIM_SUPPLIERS.SUPPLIER_ID = fsale.SUPPLIER_ID;
			IF rec=0 THEN
                INSERT INTO DIM_SUPPLIERS VALUES(fsale.SUPPLIER_ID,fsale.SUPPLIER_NAME);
            END IF;
			
			-- DIM_DATE
			SELECT COUNT(*) INTO rec FROM DIM_DATE WHERE DIM_DATE.T_DATE = fsale.T_DATE;
            IF rec=0 THEN
                INSERT INTO DIM_DATE VALUES(fsale.T_DATE,EXTRACT(YEAR FROM fsale.T_DATE),EXTRACT(MONTH FROM fsale.T_DATE));
            END IF;    

			-- FACT_SALE
            INSERT INTO FACT_SALE VALUES(FACT_SALE_ID.NEXTVAL,fsale.PRODUCT_ID,fsale.CUSTOMER_ID,fsale.WAREHOUSE_ID,
                        fsale.SUPPLIER_ID,fsale.T_DATE,fsale.QUANTITY_SOLD,fsale.SALE_PRICE,fsale.TOTAL_SALE);
            
            CLOSE c_DS_MD;
        END LOOP;
    END LOOP;
    CLOSE c_DS;
END;

