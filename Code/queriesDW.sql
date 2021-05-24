--Q1 Determine the top 3 products in Dec 2019 in terms of total sales
SELECT * FROM( SELECT P.PRODUCT_NAME,
                      SUM(F.TOTAL_SALE) AS "SUM(TOTAL_SALES)",
                      RANK() OVER (ORDER BY SUM(F.TOTAL_SALE) DESC) AS "RANK"
                FROM FACT_SALE F
                INNER JOIN DIM_PRODUCTS P
                ON F.PRODUCT_ID = P.PRODUCT_ID
                INNER JOIN DIM_DATE D
                ON F.T_DATE = D.T_DATE
                WHERE D.T_YEAR = 2019 AND D.T_MONTH = 12
                GROUP BY P.PRODUCT_NAME)
WHERE "RANK" < 4;

--Q2 Determine which customer produced highest sales in the whole year?
SELECT CUSTOMER_NAME, "SUM(TOTAL_SALES)" 
FROM ( SELECT C.CUSTOMER_NAME, 
              SUM(F.TOTAL_SALE) AS "SUM(TOTAL_SALES)",
              RANK() OVER (ORDER BY SUM(F.TOTAL_SALE) DESC) AS "RANK"
        FROM FACT_SALE F
        INNER JOIN DIM_CUSTOMERS C
        ON F.CUSTOMER_ID = C.CUSTOMER_ID 
        GROUP BY C.CUSTOMER_NAME)
WHERE "RANK" = 1

--Q3 How many sales transactions were there for the product that generated maximum sales revenue in 2019?
SELECT MS.PRODUCT_ID AS "PRODUCT_ID", 
       S.SUPPLIER_NAME, 
       COUNT(*), 
       SUM(F1.QUANTITY_SOLD) AS "SUM(QUANTITY)",
       "RANK"
FROM 
    (SELECT PRODUCT_ID, "RANK" FROM( SELECT P.PRODUCT_ID ,
                                    SUM(F.TOTAL_SALE) AS "SUM(TOTAL_SALE)",
                                    RANK() OVER (ORDER BY SUM(F.TOTAL_SALE) DESC) AS "RANK"
                            FROM FACT_SALE F
                            INNER JOIN DIM_PRODUCTS P
                            ON F.PRODUCT_ID = P.PRODUCT_ID
                            INNER JOIN DIM_DATE D
                            ON F.T_DATE = D.T_DATE
                            WHERE D.T_YEAR = 2019 
                            GROUP BY P.PRODUCT_ID) 
    WHERE "RANK" = 1) MS 
INNER JOIN FACT_SALE F1
ON MS.PRODUCT_ID = F1.PRODUCT_ID
INNER JOIN DIM_SUPPLIERS S
ON F1.SUPPLIER_ID = S.SUPPLIER_ID 
GROUP BY MS.PRODUCT_ID,S.SUPPLIER_NAME,"RANK"

--Q4 Present the quarterly sales analysis for all warehouses using drill down query concepts
SELECT WAREHOUSE_NAME,
       SUM(CASE WHEN T_MONTH IN (1,2,3) THEN BBB.TOTAL_SALE END) AS "Q1_2019",
       SUM(CASE WHEN T_MONTH IN (4,5,6) THEN TOTAL_SALE END) AS "Q2_2019",
       SUM(CASE WHEN T_MONTH IN (7,8,9) THEN TOTAL_SALE END) AS "Q3_2019",
       SUM(CASE WHEN T_MONTH IN (10,11,12) THEN TOTAL_SALE END) AS "Q4_2019"
FROM (SELECT W.WAREHOUSE_NAME,
             SUM(F.TOTAL_SALE ) AS "TOTAL_SALE",
             D.T_MONTH
      FROM FACT_SALE F
      INNER JOIN DIM_WAREHOUSE W
      ON F.WAREHOUSE_ID = W.WAREHOUSE_ID
      INNER JOIN DIM_DATE D
      ON F.T_DATE = D.T_DATE
      WHERE D.T_YEAR = 2019
      GROUP BY W.WAREHOUSE_NAME,D.T_MONTH) BBB 
GROUP BY WAREHOUSE_NAME

--Q5 Create a materialised view named “Warehouse_Analysis_mv” that presents the product-wise sales analysis for each warehouse.              
DROP materialized VIEW Warehouse_Analysis_mv;
CREATE materialized VIEW Warehouse_Analysis_mv build  IMMEDIATE 
REFRESH COMPLETE
ENABLE QUERY REWRITE
AS
    SELECT F.WAREHOUSE_ID, F.PRODUCT_ID,
    SUM(F.TOTAL_SALE) AS "TOTAL SALES"
    FROM 
        FACT_SALE F
        GROUP BY F.WAREHOUSE_ID, F.PRODUCT_ID;
select * from Warehouse_Analysis_mv;

--ROLLUP
DROP materialized VIEW Warehouse_Analysis_mv;
CREATE materialized VIEW Warehouse_Analysis_mv build  IMMEDIATE 
REFRESH COMPLETE
ENABLE QUERY REWRITE
AS
    SELECT F.WAREHOUSE_ID, F.PRODUCT_ID,
    SUM(F.TOTAL_SALE) AS "TOTAL SALES"
    FROM 
        FACT_SALE F
        GROUP BY ROLLUP (F.WAREHOUSE_ID, F.PRODUCT_ID)
        ORDER BY F.WAREHOUSE_ID, F.PRODUCT_ID;
select * from Warehouse_Analysis_mv;

--CUBE
DROP materialized VIEW Warehouse_Analysis_mv;
CREATE materialized VIEW Warehouse_Analysis_mv build  IMMEDIATE 
REFRESH COMPLETE
ENABLE QUERY REWRITE
AS
    SELECT F.WAREHOUSE_ID, F.PRODUCT_ID,
    SUM(F.TOTAL_SALE) AS "TOTAL SALES"
    FROM 
        FACT_SALE F 
        GROUP BY CUBE (F.WAREHOUSE_ID, F.PRODUCT_ID)
        ORDER BY F.WAREHOUSE_ID, F.PRODUCT_ID;
select * from Warehouse_Analysis_mv;