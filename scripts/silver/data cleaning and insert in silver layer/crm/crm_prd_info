use DataWarehouse;

/*
----------------------------------------
======================checking data in table <bronze.crm_prd_info>============================
This shows the duplicate value in our primary key cst_id then cleaning it 
---------------------------------------------------
*/
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;
select* from bronze.erp_px_cat_g1v2;
select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null
/* as checked prd_id is having no duplicates and can be used as a primary key */
/* now as checked prd_key consist of two ids i.e. product category 
from <bronze.erp_px_cat_g1v2> table & product key from <bronze.crm_sales_details> table */

select
prd_id,
replace(substring(prd_key,1,5), '-','_') as cat_id, ---------- extract category ID
SUBSTRING(prd_key,7,len(prd_key)) as prd_key, ------------------ extract product key
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
case 
when upper(trim(prd_line)) = 'M' then 'Mountain'
when upper(trim(prd_line)) = 'R' then 'Road'
when upper(trim(prd_line)) = 'S' then 'Other Sales'
when upper(trim(prd_line)) = 'T' then 'Touring'
else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,-------- data type casting
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt---- 
/* calculate end date as one day before the next start date */
from bronze.crm_prd_info



where SUBSTRING(prd_key,7,len(prd_key)) in 
(select sls_prd_key from bronze.crm_sales_details)

WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT DISTINCT id 
    FROM bronze.erp_px_cat_g1v2
)


/* checking unwanted spaces for table prd_nm */
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)
/* we found no spaces */

/* checking Null or negative for table prd_cost */
select prd_cost
from bronze.crm_prd_info
where prd_cost is null or prd_cost < 0
/* we found no negative values but nulls */

/* data standardization & consistency 
check data in prd_line
*/

select distinct prd_line
from bronze.crm_prd_info

/* in this data this query shows us that we have Five values i.e. M, R, S, T, Null */

/* checking quality of date , invalid date orders */

select *,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info
/*where prd_key in ('AC-HE-HL-U509-R' ,'AC-HE-HL-U509')*/



/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===================================silver.crm_prd_info=====================================*/

/*=============================================================================*/

Insert into silver.crm_prd_info(
prd_id,
Cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select
prd_id,
replace(substring(prd_key,1,5), '-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
case 
when upper(trim(prd_line)) = 'M' then 'Mountain'
when upper(trim(prd_line)) = 'R' then 'Road'
when upper(trim(prd_line)) = 'S' then 'Other Sales'
when upper(trim(prd_line)) = 'T' then 'Touring'
else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 
as date
) as prd_end_dt ------- calculate end date as one day before the next start date
from bronze.crm_prd_info

/*=============================================================================*/


/* ======================******** CHECKING DATA OF SILVER LAYER *********=========================*/
/*===================================silver.crm_prd_info=====================================*/

/*=============================================================================*/

SELECT * FROM SILVER.crm_prd_info;

/* checking Null or negative for table prd_cost */
select prd_cost
from SILVER.crm_prd_info
where prd_cost is null or prd_cost < 0
/* we found no negative & no nulls */

/* data standardization & consistency 
check data in prd_line
*/

select distinct prd_line
from silver.crm_prd_info

/* in this data this query shows us that we have Five values i.e. Mountain, Road, Other Sales, Touring, n/a */

/* checking quality of date , invalid date orders */

select *
from silver.crm_prd_info
where prd_start_dt >prd_end_dt

/* as checked no start date is greater than end date */
/* silver table passed all the checkes */
