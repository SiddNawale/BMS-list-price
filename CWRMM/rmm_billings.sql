WITH brands as (
     SELECT  obt.COMPANY_ID,
               obt.REPORTING_DATE,
                'Automate' as Brand,
             MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE='Automate'
    and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ='Automate'
    and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-', 'Standard','Internal IT')
    and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in ('SaaS', 'Maintenance', 'On Premise (Subscription)')
    , UNITS,0))         AS UNITS,
               sum(MRR)                                           AS MRR
        FROM ANALYTICS.DBO.GROWTH__OBT obt
        WHERE METRIC_OBJECT = 'applied_billings'
          AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
      group by 1,2

    union all
     SELECT  obt.COMPANY_ID,
               obt.REPORTING_DATE,
                'Command' as Brand,
               sum(UNITS)                                        AS UNITS,
               sum(MRR)                                           AS MRR
        FROM ANALYTICS.DBO.GROWTH__OBT obt
        WHERE METRIC_OBJECT = 'applied_billings'
          AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'

          group by 1,2
    union all
     SELECT  obt.COMPANY_ID,
               obt.REPORTING_DATE,
                'CW RMM' as Brand,
               sum(UNITS)                                        AS UNITS,
               sum(MRR)                                           AS MRR
        FROM ANALYTICS.DBO.GROWTH__OBT obt
        WHERE METRIC_OBJECT = 'applied_billings'
          AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'CW RMM'
    group by 1,2
    )
        SELECT COMPANY_ID,
               REPORTING_DATE,
               Brand,
               SUM(UNITS)                                         AS UNITS,
               SUM(MRR)                                           AS MRR
        FROM brands
        where REPORTING_DATE<=(select distinct
                                case
                                when day(CURRENT_DATE()) > 3
                                then date_trunc('Month',add_months(CURRENT_DATE()::date, -1))
                                else date_trunc('Month',add_months(CURRENT_DATE()::date, -2))
                                end as date)
        GROUP BY 1, 2, 3