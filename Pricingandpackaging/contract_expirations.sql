Select company_id,
       product_categorization_arr_reported_product,
       reporting_date,

       max(contract_end_date) as contract_end_date

from ANALYTICS.DBO.GROWTH__OBT
where 1 = 1
  and product_categorization_arr_reported_product <> '🙈 nothing to see here'
  and contract_end_date is not null
group by company_id, product_categorization_arr_reported_product, reporting_date