with base as
         (SELECT REPORTING_DATE,
                 COMPANY_ID,
                 PRODUCT_CATEGORIZATION_PRODUCT_LINE,
                 PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE,
                 ITEM_DESCRIPTION,
                 PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE,

                 max(IFF((PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                          PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage')
                             and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%MAINTENANCE%' or
                                  upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%ASSURANCE%'), 1,
                         0))                                                                                        as Base_Manage_Legacy_On_Prem,
                 max(IFF((PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                          PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage')
                             and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SUBSCRIPTION%'), 1,
                         0))                                                                                        as Base_Manage_On_Prem,
                 max(IFF((PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                          PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage')
                             and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SAAS%'), 1,
                         0))                                                                                        as Base_Manage_Cloud,

                 max(IFF(((PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                           PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers'))
                     and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%MAINTENANCE%' or
                          upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%ASSURANCE%')), 1,
                         0))                                                                                        as Base_Automate_Legacy_On_Prem,

                 max(IFF(((PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                           PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers'))
                     and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SUBSCRIPTION%')), 1,
                         0))                                                                                        as Base_Automate_On_Prem,

                 max(IFF(((PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                           PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers'))
                     and (upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SAAS%')), 1,
                         0))                                                                                        as Base_Automate_Cloud
          FROM ANALYTICS.DBO.GROWTH__OBT
          WHERE REPORTING_DATE = (select distinct case
                                                      when day(CURRENT_DATE()) > 2
                                                          then date_trunc('Month', add_months(CURRENT_DATE()::date, -1))
                                                      else date_trunc('Month', add_months(CURRENT_DATE()::date, -2))
                                                      end as date)


            AND METRIC_OBJECT = 'applied_billings'
            AND Company_name <> ''
            and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Manage', 'Automate', 'Command')
          GROUP BY 1, 2, 3, 4, 5, 6
          HAVING SUM(BILLINGS) > 0),
     intermediate1 as (
         select COMPANY_ID,
                case
                    when Base_Manage_Legacy_On_Prem = 1 then 1
                    when Base_Manage_On_Prem = 1 then 2
                    when Base_Manage_Cloud = 1 then 3 end   as Manage_Category,
                case
                    when Base_Automate_Legacy_On_Prem = 1 then 1
                    when Base_Automate_On_Prem = 1 then 2
                    when Base_Automate_Cloud = 1 then 3 end as Automate_Category
         from base),
     intermediate2 as (
         select COMPANY_ID,
                min(Manage_Category)   as Manage_Category,
                min(Automate_Category) as Automate_Category
         from intermediate1
         group by 1),
     intermediate3 as (
         select COMPANY_ID,
                case
                    when Manage_Category = 1 then 'Legacy_On_Prem'
                    when Manage_Category = 2 then 'On_Prem'
                    when Manage_Category = 3 then 'Cloud'
                    else 'NA'
                    end as Manage_Category,
                case
                    when Automate_Category = 1 then 'Legacy_On_Prem'
                    when Automate_Category = 2 then 'On_Prem'
                    when Automate_Category = 3 then 'Cloud'
                    else 'NA'
                    end as Automate_Category
         from intermediate2),
     intermediate4 as (
         select distinct COMPANY_ID, Manage_Category, Automate_Category from intermediate3),
     intermediate5 as (select distinct intermediate4.company_id,
                                       manage_category,
                                       automate_category,
                                       iff(Manage_Category <> 'NA' and Manage_Category = 'Legacy_On_Prem', 1, 0)     as PSA_LEGACY_ON_PREM,
                                       iff(Manage_Category <> 'NA' and Manage_Category = 'On_Prem', 1, 0)            as PSA_ON_PREM,
                                       iff(Manage_Category <> 'NA' and Manage_Category = 'Cloud', 1, 0)              as PSA_CLOUD,
                                       iff(Automate_Category <> 'NA' and Automate_Category = 'Legacy_On_Prem', 1,
                                           0)                                                                        as AUTOMATE_LEGACY_ON_PREM,
                                       iff(Automate_Category <> 'NA' and Automate_Category = 'On_Prem', 1, 0)        as AUTOMATE_ON_PREM,
                                       iff(Automate_Category <> 'NA' and Automate_Category = 'Cloud', 1, 0)          as AUTOMATE_CLOUD,
                                       REPORTING_DATE, --expanded columns and included the arr table columns
                                       PRODUCT,
                                       package,
                                       "Item Description",
                                       ITEM_ID,
                                       CATEGORY,
                                       arr,
                                       UNITS,
                                       MRR,
                                       "Brand",
                                       "Seat Type",
                                       UNITS_INCLUDE_FLAG

                       from intermediate4
                                left join DATAIKU.DEV_DATAIKU_STAGING.PNP_dashboard_ARR_AND_BILLING_C arr -- merged queries step 1
                                          on intermediate4.COMPANY_ID = arr.COMPANY_ID
                       where REPORTING_DATE =
                             (select max(REPORTING_DATE) from DATAIKU.DEV_DATAIKU_STAGING.PNP_dashboard_ARR_AND_BILLING)
                       group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
     intermediate6 as (select company_id,
                              manage_category,
                              automate_category,
                              psa_legacy_on_prem,
                              psa_on_prem,
                              psa_cloud,
                              automate_legacy_on_prem,
                              automate_on_prem,
                              automate_cloud,
                              SUM(iff("Brand" = 'Manage' and "Seat Type" = 'Include in Current ARR calculation', arr,
                                      0))                                                                             as PSA_ARR,        --psa arr
                              sum(iff("Brand" = 'Automate' or PACKAGE = 'Desktops' or PACKAGE = 'Networks' or
                                      PACKAGE = 'Servers', ARR,
                                      0))                                                                             as AUTOMATE_ARR,   --automate arr
                              sum(iff("Brand" = 'Manage' and UNITS_INCLUDE_FLAG = 'Include in Unit Count', UNITS,
                                      0))                                                                             as PSA_UNITS,      --psa units
                              sum(iff("Brand" = 'Automate' or PACKAGE = 'Desktops' or PACKAGE = 'Networks' or
                                      PACKAGE = 'Servers', UNITS,
                                      0))                                                                             as AUTOMATE_UNITS, --automate units
                              sum(iff("Brand" = 'Automate', UNITS, 0))                                                as AUTOMATE_ONLY_UNITS


                       from intermediate5

                       group by 1, 2, 3, 4, 5, 6, 7, 8, 9)

select *,
       iff(Manage_Category = 'On_Prem', PSA_ARR, 0)                 as PSA_ON_PREM_ARR,
       iff(Manage_Category = 'Cloud', PSA_ARR, 0)                   as PSA_CLOUD_ARR,
       iff(Manage_Category = 'Legacy_On_Prem', PSA_ARR, 0)          as PSA_LEGACY_ON_PREM_ARR,


       iff(Manage_Category = 'On_Prem', PSA_UNITS, 0)               as PSA_ON_PREM_UNITS,
       iff(Manage_Category = 'Cloud', PSA_UNITS, 0)                 as PSA_CLOUD_UNITS,
       iff(Manage_Category = 'Legacy_On_Prem', PSA_UNITS, 0)        as PSA_LEGACY_ON_PREM_UNITS,


       iff(Automate_Category = 'Cloud', AUTOMATE_ARR, 0)            as AUTOMATE_CLOUD_ARR,
       iff(Automate_Category = 'On_Prem', AUTOMATE_ARR, 0)          as AUTOMATE_ON_PREM_ARR,
       iff(Automate_Category = 'Legacy_On_Prem', AUTOMATE_ARR, 0)   as AUTOMATE_LEGACY_ON_PREM_ARR,

       iff(Automate_Category = 'Cloud', AUTOMATE_UNITS, 0)          as AUTOMATE_CLOUD_UNITS,
       iff(Automate_Category = 'On_Prem', AUTOMATE_UNITS, 0)        as AUTOMATE_ON_PREM_UNITS,
       iff(Automate_Category = 'Legacy_On_Prem', AUTOMATE_UNITS, 0) as AUTOMATE_LEGACY_ON_PREM_UNITS
from intermediate6
--where COMPANY_ID = '0016g00000pTweXAAS'

