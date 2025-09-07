select * from bronze.crm_sales_details;

select 
sls_ord_num from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

 
select 
sls_prd_key from bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key)

/* checking connections through primary key */

select 
* from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

select 
* from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

/* everythink is correct with the first 3 columns and primary key is also same */

/* checking for the order dt */

select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8 or sls_order_dt > 20500101 or sls_order_dt < 19990101;

/* we have found 19 records that are not a correct date or in correct format */
/* we will make all this null while data cleaning */

/* checking for the ship dt */

select 
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or len(sls_ship_dt) != 8 or sls_ship_dt > 20500101 or sls_ship_dt < 19990101;

/* we have found no wrong data still will apply the check to make it furture proof */
/* we will just covert the date in correct format */


/* checking for the due dt */

select 
nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or len(sls_due_dt) != 8 or sls_due_dt > 20500101 or sls_due_dt < 19990101;

/* we have found no wrong data still will apply the check to make it furture proof */
/* we will just covert the date in correct format */

/* check for ship or due date hi older than order date */

select * 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

/* data is clear and no modification is needed */


/* checking for the sales */

Select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is  null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 OR sls_price <=0
order by sls_sales, sls_quantity, sls_price

/* it gicves 33 records with wrong data */

/* fixing the wrong data */

Select distinct
sls_sales as old_sales,
sls_quantity,
sls_price as old_price,

case when sls_sales is null or sls_sales <=0 or sls_Sales != sls_quantity * abs(sls_price)
       then sls_quantity * abs(sls_price)
       else sls_sales
       end as sls_sales,

case when sls_price is null or sls_price <=0
       then sls_sales/ sls_quantity
       else sls_price
       end as sls_price
from bronze.crm_sales_details



/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===================================silver.crm_sales_details=====================================*/

/*=============================================================================*/

insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null ---- handling invalid data
else cast(cast(sls_order_dt as varchar) as date)---- data type casting 
end as sls_order_dt,
--------------------------------------------------------------
case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null ------ handling invalid data 
else cast(cast(sls_ship_dt as varchar) as date) --- data type casting
end as sls_ship_dt,
-------------------------------------------------------------------
case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
---------------------------------------------------------------------
case when sls_sales is null or sls_sales <=0 or sls_Sales != sls_quantity * abs(sls_price)
       then sls_quantity * abs(sls_price)
       else sls_sales
       end as sls_sales,--- calculating sales if original value is missing or incorrect 
sls_quantity,
case when sls_price is null or sls_price <=0
       then sls_sales/ sls_quantity
       else sls_price--- derive price if original value is invalid
       end as sls_price

from bronze.crm_sales_details

/*=============================================================================*/


/* ======================******** CHECKING DATA OF SILVER LAYER *********=========================*/
/*===================================silver.crm_sales_details=====================================*/

/*=============================================================================*/

select * from silver.crm_sales_details


/* checking for the order dt, ship dt & due dt */

select * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

/* no invalid date */

/* checking for sales, quantity & price */

Select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is  null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 OR sls_price <=0
order by sls_sales, sls_quantity, sls_price

/* it doesn't gives any invalid data */
