## Operating the Data Warehouse
The operation of the data warehouse can be broken down into 3 major steps:
1) Build a DW
2) Enrich Customer Transaction Data and Store in DW, extraction, transformation, and loading using the INLJ algorithm.

## Requirements
The `DIM_CUSTOMERS`, `DIM_WAREHOUSE`, `DIM_PRODUCTS`, `DIM_SUPPLIERS`, `DIM_DATE` and `FACT_SALE` tables should already be loaded in the database.

## Part 1: Build a DW 
CREATE the necessary tables for the data warehouse accoring to the STAR-Schema. All existing tables with the same name will be dropped and replaced by empty 
tables named `DIM_CUSTOMERS`, `DIM_WAREHOUSE`, `DIM_PRODUCTS`, `DIM_SUPPLIERS`, `DIM_DATE` (dimension tables); and `FACT_SALE` (fact table).

Once completed, all tables aboved would have been created.

## Part 2: ETL Using The INLJ Algorithm
1) The algorithm involves the creation of the `c_DS` which is a cursor that will be used to read the tuples from the `DATASTREAM` table.
2) The `BULK COLLECT...LIMIT=100` statement limits the number of tuples per batch read to **100**.
3) It is possible to change this by replacing the number "100" by any number less than the total number of records in the `DATASTREAM` table.

Once completed, the script would have loaded the data in the relevant Data Warehouse tables.

## Part 3: Multidimensional Data Analysis
1) Query the top 3 products in Dec 2019 in terms of total sales
2) Query which customer produced highest sales in the whole year
3) Counting quantity sold for the product that generated maximum sales revenue in 2019
4) Present the quarterly sales analysis for all warehouses using drill down query concepts
5) Create a materialised view named “Warehouse_Analysis_mv” that presents the product-wise sales analysis for each warehouse. And using "ROLLUP" AND "CUBE" to see the difference          
