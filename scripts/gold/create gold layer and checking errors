===============================================================================
Script: Create Gold Views
===============================================================================
🎯 Script Purpose
- Builds views for the Gold layer in the data warehouse
- Gold layer contains the final dimension and fact tables in a Star Schema format
- Transforms and combines data from the Silver layer
- Produces datasets that are clean, enriched, and ready for business use
📊 Usage
- Views are designed for direct querying
- Ideal for analytics, dashboards, and reporting
- Provides a reliable foundation for business insights

===============================================================================
*/



select 
ci.cst_id,
ci.cst_key,
ci.Cst_firstname,
ci.cst_lastname,
ci.cst_maritial_status,
ci.Cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid





/* checking the joins are correct and data is not invalid */
select cst_id, count(*)from 
(
select 
ci.cst_id,
ci.cst_key,
ci.Cst_firstname,
ci.cst_lastname,
ci.cst_maritial_status,
ci.Cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
)t group by cst_id
having count(*) >1

/* as checked we haven't found any duplicate data after joins */

/* as checking the data we found that we have two column with same information i.e. data gender */


select distinct
ci.Cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
     else coalesce(ca.gen, 'n/a')
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2

/* as checked the data in 10 records missmatch in the both table
 we need to perform data integration */
 /* using the below sql script we integrated the data 
     case when ci.cst_gndr != 'n/a' then ci.cst_gndr
     else coalesce(ca.gen, 'n/a')
end as new_gen
*/

-- =============================================================================
-- Create Dimension Table: gold.dim_customer
-- =============================================================================
Create view gold.dim_customer as 
select 
row_number () over (order by ci.cst_id) as customer_key, --- Assigning a serrogate key
ci.cst_id as Customer_ID,
ci.cst_key as Customer_number,
ci.Cst_firstname As First_Name,
ci.cst_lastname as Last_Name,
la.cntry as Country,
ci.cst_maritial_status as Maritial_Status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
     else coalesce(ca.gen, 'n/a')
end as Gender,
ca.bdate as BirthDate,
ci.cst_create_date as Create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

/* checking the quality of the view */

select * from gold.dim_customer

select distinct 
gender
from gold.dim_customer

/* Building the second join of the product information i.e. Crm_prd_info & erp_px_cat_g1v2 */


select * from silver.erp_px_cat_g1v2


select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null --- filtering historical data 


/* checking the joins are correct and data is not invalid */

select prd_key, count(*) from(
select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null
) t group by prd_key
having count(*) >1

/* as checked we have no duplication of data */
/* creating view and dimension table */
-- =============================================================================
-- Create Dimension Table: gold.dim_products
-- =============================================================================
create view gold.dim_products as
select 
row_number() over (order by pn.prd_start_dt,pn.prd_key) as Product_key, --- Assiging the serrogate key
pn.prd_id as Product_id,
pn.prd_key as Product_number,
pn.prd_nm as Product_name,
pn.cat_id as Category_id,
pc.cat as Category,
pc.subcat as SubCategory,
pc.maintenance,
pn.prd_cost as Product_Cost,
pn.prd_line as Product_line,
pn.prd_start_dt as Start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null

/* checking the view */

select * from gold.dim_products
select * from gold.dim_customer
select * from silver.crm_sales_details

/* now creating the Fact table 
using the dimension's surrogate keys instead of IDs to easily connect facts with dimensions*/

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================


create view gold.fact_sales as
select 
sd.sls_ord_num as Order_Number,
pd.Product_key,
cu.customer_key,
sd.sls_order_dt as Order_Date,
sd.sls_ship_dt as Shipping_Date,
sd.sls_due_dt as Due_date,
sd.sls_sales as Sales_Amount,
sd.sls_quantity as Quantity,
sd.sls_price as Price
from silver.crm_sales_details sd
left join gold.dim_products pd
on sd.sls_prd_key = pd.Product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.Customer_ID


/* quality check of gold fact table */

Select * from gold.fact_sales

/* foreign key integrity ( Dimensions ) */

select * 
from gold.fact_sales f
left join gold.dim_customer c
on c.customer_key = f.customer_key
where c.customer_key is null


select * 
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.Product_key
where p.Product_key is null

/* it returns the no invalid data */
