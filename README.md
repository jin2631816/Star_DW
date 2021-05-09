## Operating the Data Warehouse
The operation of the data warehouse can be broken down into 3 major steps:
1) Build a DW
2) Enrich Customer Transaction Data and Store in DW, extraction, transformation, and loading using the INLJ algorithm.
3) To be continued

## Requirements
The `DIM_CUSTOMERS`, `DIM_WAREHOUSE`, `DIM_PRODUCTS`, `DIM_SUPPLIERS`, `DIM_DATE` and `FACT_SALE` tables should already be loaded in the database.

## Part 1: Build a DW 
1) Open *SQLDeveloper*.
2) PLEASE RE RUN THE SQL SCRIPT FILE *CREATEDW.SQL* WHICH WILL CREATE THE NECESSARY TABLES FOR THE DATA WAREHOUSE ACCORDING TO THE STAR-SCHEMA.
   WE HAVE ADDED ONE PRIMARY KEY `FACT_SALE_ID` FOR `FACT_SALE` (FACT TABLE) AS ADVISED AND CORRECTED SOME MISTAKES AS WELL.
   All existing tables with the same name will be dropped and replaced by empty tables named
   `DIM_CUSTOMERS`, `DIM_WAREHOUSE`, `DIM_PRODUCTS`, `DIM_SUPPLIERS`, `DIM_DATE` (dimension tables); and `FACT_SALE` (fact table).

Once completed, all tables aboved would have been created.

## Part 2: Extraction, Transformation, & Loading Using The INLJ Algorithm
1) With *SQLDeveloper* open, open and run the *INLJ.sql* PL-SQL file which will implement the INLJ algorithm.
2) The algorithm involves the creation of the `c_DS` which is a cursor that will be used to read the tuples from the `DATASTREAM` table.
3) The `BULK COLLECT...LIMIT=100` statement limits the number of tuples per batch read to **100**.
4) It is possible to change this by replacing the number "100" by any number less than the total number of records in the `DATASTREAM` table.

Once completed, the script would have loaded the data in the relevant Data Warehouse tables.

## Part 3: Multidimensional Data Analysis
1) Query the top 3 products in Dec 2019 in terms of total sales
2) Query which customer produced highest sales in the whole year
3) Counting quantity sold for the product that generated maximum sales revenue in 2019
4) Present the quarterly sales analysis for all warehouses using drill down query concepts
5) Create a materialised view named “Warehouse_Analysis_mv” that presents the product-wise sales analysis for each warehouse. And using "ROLLUP" AND "CUBE" to see the difference          
