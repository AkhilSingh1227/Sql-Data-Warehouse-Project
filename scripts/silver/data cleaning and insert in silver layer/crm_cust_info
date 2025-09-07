use DataWarehouse;

/*
----------------------------------------
======================checking data in table <bronze.crm_cust_info>============================
This shows the duplicate value in our primary key cst_id then cleaning it 
---------------------------------------------------
*/
select
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null
/* -------------------------------------------------------------------------*/

select 
* 
from bronze.crm_cust_info
WHERE cst_id = 29466;
/*
-----------------------------------------
taking one cst_id to check why we have duplicates in the data.
As here we can see the cst_create_date is different 
------------------------------------------------
*/

SELECT *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info
where cst_id = 29466
/* ----------------------------------------------------------
Arraning the data in desc order i.e. the latest date should be taken
---------------------------------------------------------------*/ 

	select * 
	from 
	(
	SELECT *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	)t where flag_last = 1

	/* 
-------------------------------------------------------------------------------------------
- ROW_NUMBER(): Assigns a unique sequential number to each row within a partition.
----The OVER clause is used with window functions like ROW_NUMBER(), RANK(), SUM(), etc. 
----It defines how the function should be applied across rows.
- PARTITION BY cst_id: Groups the data by customer ID. Each customer gets their own partition.
- ORDER BY cst_create_date DESC: Within each customer group, rows are ordered by creation date, newest first.
- flag_last: The alias for the row number. So the most recent record for each customer will have flag_last = 1.
---------------------------------------------------------------------------------------------
*/

/* checking unwanted spaces for all the string values in the table */

select Cst_firstname
from bronze.crm_cust_info
where Cst_firstname != Trim(cst_firstname)
/* we found 15 records that have spaces in starting or ending 
---------------------------------------------------------------------------------*/

/* checking same for last name */
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != Trim(cst_lastname)
/* we found 17 records that have spaces in starting or ending 
------------------------------------------------------------------------------------*/

/* checking same for gender */
select cst_gndr
from bronze.crm_cust_info
where cst_gndr != Trim(cst_gndr)
/* this data is clean and don't have any spaces.
----------------------------------------------------------------------------------------*/
/* checking same for maritial status */
select cst_maritial_status
from bronze.crm_cust_info
where cst_maritial_status != Trim(cst_maritial_status)
/* this data is clean and don't have any spaces.*/

/* checking same for cst_key */
select cst_key
from bronze.crm_cust_info
where cst_key != Trim(cst_key)
/* this data is clean and don't have any spaces.*/

/* data standardization & consistency 
check data in Cst_gndr and Cst_maritial status
*/

select distinct cst_gndr
from bronze.crm_cust_info
/* in this data this query shows us that we have three values i.e. M, F, Null */
select distinct cst_maritial_status
from bronze.crm_cust_info
/* in this data this query shows us that we have three values i.e. M, S, Null */

/* now lets remove spaces from the tow column lastname and first name */

/*=======================================================================
This query removes spaces from firstname & last name also clean cst_id as unique and satndardized data for 
the gender and maritial status
==========================================================================
*/
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_gndr)) ='M' then 'Male'
when upper(trim(cst_gndr)) ='F' then 'Female'
else 'n/a'
end as cst_gndr,
case when upper(trim(cst_maritial_status)) ='S' then 'Single'
when upper(trim(cst_maritial_status)) ='M' then 'Married'
else 'n/a'
end as cst_maritial_status,
Cst_create_date
From(
	SELECT *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	)t where flag_last = 1
/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===========================================================================================*/
truncate table silver.crm_cust_info;
Insert into silver.crm_cust_info
(cst_id,
cst_key,
Cst_firstname,
cst_lastname,
cst_maritial_status,
Cst_gndr, 
cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_maritial_status)) ='S' then 'Single'
when upper(trim(cst_maritial_status)) ='M' then 'Married'
else 'n/a'
end as cst_maritial_status,

case when upper(trim(cst_gndr)) ='M' then 'Male'
when upper(trim(cst_gndr)) ='F' then 'Female'
else 'n/a'
end as cst_gndr,

Cst_create_date
From(
	SELECT *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	)t where flag_last = 1

/*=========================== PERFORMING CHECKS ON SILVER.CRM_CUST_INFO =========================*/
/*  checking data in table <silver.crm_cust_info>
This shows the NO duplicate value in our primary key cst_id
---------------------------------------------------
*/
select
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

select * from silver.crm_cust_info;
/*
DELETE FROM silver.crm_cust_info
WHERE cst_key IN (
    SELECT TOP 1 cst_key
    FROM silver.crm_cust_info);
	*/


/* checking unwanted spaces for all the string values in the table */



select Cst_firstname
from silver.crm_cust_info
where Cst_firstname != Trim(cst_firstname)
/* we found no spaces 
---------------------------------------------------------------------------------*/

/* checking same for last name */
select cst_lastname
from silver.crm_cust_info
where cst_lastname != Trim(cst_lastname)
/* we found no spaces 
------------------------------------------------------------------------------------*/

/* data standardization & consistency 
check data in Cst_gndr and Cst_maritial status
*/

select distinct cst_gndr
from silver.crm_cust_info
/* in this data this query shows us that we have three values i.e. Male, Female, n/a */
select distinct cst_maritial_status
from silver.crm_cust_info
/* in this data this query shows us that we have three values i.e. Married, Single, N/a */

