DROP TABLE FACT_SALE;
DROP TABLE DIM_CUSTOMERS;
DROP TABLE DIM_WAREHOUSE;
DROP TABLE DIM_PRODUCTS;
DROP TABLE DIM_SUPPLIERS;
DROP TABLE DIM_DATE;

--Create dimension tables - CUSTOMERS, WAREHOUSE, PRODUCTS, SUPPLIERS DATE tables
CREATE TABLE DIM_CUSTOMERS(
    CUSTOMER_ID VARCHAR2(4) NOT NULL PRIMARY KEY,
    CUSTOMER_NAME VARCHAR2(30) NOT NULL
);

CREATE TABLE DIM_WAREHOUSE(
    WAREHOUSE_ID VARCHAR2(4) NOT NULL PRIMARY KEY,
    WAREHOUSE_NAME VARCHAR2(20) NOT NULL
);

CREATE TABLE DIM_PRODUCTS(
    PRODUCT_ID VARCHAR2(6) NOT NULL PRIMARY KEY,
    PRODUCT_NAME VARCHAR2(30) NOT NULL
);

CREATE TABLE DIM_SUPPLIERS(
    SUPPLIER_ID VARCHAR2(5) NOT NULL PRIMARY KEY,
    SUPPLIER_NAME VARCHAR2(30) NOT NULL
);
CREATE TABLE DIM_DATE(
    T_DATE    DATE NOT NULL PRIMARY KEY,
    T_YEAR    NUMBER(10,0),
    T_MONTH   NUMBER(10,0)
); 

--Create the fact table
CREATE TABLE FACT_SALE (
    FACT_SALE_ID NUMBER(8) NOT NULL PRIMARY KEY,
    PRODUCT_ID VARCHAR2(6) NOT NULL,
    CUSTOMER_ID VARCHAR2(4) NOT NULL,
    WAREHOUSE_ID VARCHAR2(4) NOT NULL,
    SUPPLIER_ID VARCHAR2(5) NOT NULL,
    T_DATE DATE NOT NULL,
    QUANTITY_SOLD NUMBER(3) NOT NULL,
    SALE_PRICE NUMBER(5,2) NOT NULL,
    TOTAL_SALE NUMBER(8,2) NOT NULL,
    FOREIGN KEY(CUSTOMER_ID) REFERENCES DIM_CUSTOMERS(CUSTOMER_ID) ON DELETE SET NULL,
    FOREIGN KEY(WAREHOUSE_ID) REFERENCES DIM_WAREHOUSE(WAREHOUSE_ID) ON DELETE SET NULL,
    FOREIGN KEY(PRODUCT_ID) REFERENCES DIM_PRODUCTS(PRODUCT_ID) ON DELETE SET NULL,
    FOREIGN KEY(SUPPLIER_ID) REFERENCES DIM_SUPPLIERS(SUPPLIER_ID) ON DELETE SET NULL,
	FOREIGN KEY(T_DATE) REFERENCES DIM_DATE(T_DATE) ON DELETE SET NULL
);