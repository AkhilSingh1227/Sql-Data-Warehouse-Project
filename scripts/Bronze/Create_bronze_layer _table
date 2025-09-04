===============================================================================
Code: Create Bronze Tables
===============================================================================
Use DataWarehouse;
go

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
    /*
- OBJECT_ID() is a system function that returns the ID of a database object.
- 'U' stands for User Table (as opposed to views, procedures, etc.).
- If the table exists, this returns a non-null value.

    */

go
create table bronze.crm_cust_info(
cst_id Int,
cst_key nvarchar (50),
Cst_firstname nvarchar (50),
cst_lastname nvarchar (50),
cst_maritial_status nvarchar (50),
Cst_gndr nvarchar (50),
cst_create_date Date,
);
---------------------------------------------------
go
DROP TABLE IF exists bronze.crm_prd_info;

go
cREATE TABLE bronze.crm_prd_info(

prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt DATETIME, 
);
go
---------------------------------------------------------------

    DROP TABLE if exists bronze.crm_sales_details;
go
Create TABLE bronze.crm_sales_details (

sls_ord_num nvarchar (50),
sls_prd_key nvarchar (50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,

);
go
----------------------------------------------------------------------
----------------------------------------------------------------------

    DROP TABLE if exists bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
go

    DROP TABLE if exists bronze.erp_cust_az12;
go

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO


    DROP TABLE if exists bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
go
