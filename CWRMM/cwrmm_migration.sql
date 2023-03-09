-- SET (base_product,product_1,product_2)=('CW RMM', 'Automate','Command');

-- Loading old product billings, filtering on max date of old product billings
-- Loading RMM billings, filtering on max date of RMM billings
-- Checking Price and Units difference of partners from old product to CWRMM


WITH migrated_product as (
    SELECT
        obt.COMPANY_ID,
        obt.REPORTING_DATE,
        plm."Product Name New" as ITEM_DESCRIPTION,
        sum(obt.UNITS) AS RMM_UNITS,
        sum(obt.MRR) AS RMM_MRR,
        max(obt.REPORTING_DATE) over(partition by COMPANY_ID) AS MAX_RMM_DATE,
        min(obt.REPORTING_DATE) over(partition by COMPANY_ID) AS MIN_RMM_DATE
    FROM
        ANALYTICS.DBO.GROWTH__OBT obt
        left join DATAIKU.PRD_DATAIKU_WRITE."CW_RMM_POST_LAUNCH_PRODUCT_MAPPING" plm on obt.ITEM_ID = plm.product_code
    where
        METRIC_OBJECT = 'applied_billings'
        AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'CW RMM'
        and ITEM_DESCRIPTION not ilike '%command%' and ITEM_DESCRIPTION not ilike '%core%'
        and REPORTING_DATE <=(
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        )
    group by
        1,
        2,
        3
),
old_product_1 as (
    SELECT
        distinct obt.COMPANY_ID,
        obt.REPORTING_DATE,
        max(UNITS) AS OLD_PROD_UNITS,
        sum(MRR) AS OLD_PROD_MRR,
        (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        ) as CHECK_DATE_OLD_PROD
    FROM
        ANALYTICS.DBO.GROWTH__OBT obt
    WHERE
        METRIC_OBJECT = 'applied_billings'
        AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
        and REPORTING_DATE <= (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        )
        and REPORTING_DATE >= ('2021-01-01')
        -- filter to limit old product range
    GROUP BY
        1,
        2
),
old_product_1_max_date as(
--     max of Automate date
    select
        COMPANY_ID,
        max(REPORTING_DATE) as rd
    from
        old_product_1
    group by
        1
),
old_product_1_pre_migration_price as (
--     Less than min of CWRMM date price
    select
        a.COMPANY_ID,
        a.REPORTING_DATE,
        max(a.REPORTING_DATE) as PRE_MIG_MAX_OLD_PROD,
        sum(OLD_PROD_MRR) as PRE_MIGRATION_OLD_PROD_MRR,
        sum(OLD_PROD_MRR) * 12 as PRE_MIGRATION_OLD_PROD_ARR,
        sum(OLD_PROD_UNITS) as PRE_MIGRATION_OLD_PROD_UNITS,
        row_number() over (
            partition by a.COMPANY_ID
            order by
                a.REPORTING_DATE desc
        ) as pre_migration_month_number_desc,
        1 as PRE_MIGARTION_FLAG
    from
        old_product_1 a
        inner join (
            select
                distinct COMPANY_ID,
                MIN_RMM_DATE
            from
                migrated_product
        ) rmm on a.COMPANY_ID = rmm.COMPANY_ID
        and a.REPORTING_DATE < rmm.MIN_RMM_DATE
    group by
        1,
        2
    having
        sum(OLD_PROD_MRR) > 0
),
old_prod_1_max_date as (
    select
        old_product_1.*,
        1 as OLD_PROD_FLAG
    from
        old_product_1
        inner join old_product_1_max_date on old_product_1.COMPANY_ID = old_product_1_max_date.COMPANY_ID
        and old_product_1.REPORTING_DATE = old_product_1_max_date.rd
),
max_migrated_product as (
    select
        COMPANY_ID,
        max(REPORTING_DATE) as rd
    from
        migrated_product
    group by
        1
),
migrated_prod_agg as (
    select
        migrated_product.COMPANY_ID as rmm_company_id,
        migrated_product.ITEM_DESCRIPTION,
        migrated_product.REPORTING_DATE as rmm_reporting_date,
        migrated_product.MIN_RMM_DATE,
        1 as rmm_flag,
        (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        ) as check_date_rmm,
        sum(RMM_UNITS) as RMM_UNITS,
        sum(RMM_MRR) as RMM_MRR
    from
        migrated_product
        inner join max_migrated_product on migrated_product.company_id = max_migrated_product.COMPANY_ID
        and migrated_product.REPORTING_DATE = max_migrated_product.rd
    where
        ITEM_DESCRIPTION is not null
    group by
        1,
        2,
        3,
        4,
        5
),
migrated_product_agg_others as (
    select
        migrated_product.COMPANY_ID as rmm_company_id,
        migrated_product.REPORTING_DATE as rmm_others_reporting_date,
        1 as rmm_other_flag,
        sum(RMM_UNITS) as RMM_OTHER_UNITS,
        sum(RMM_MRR) as RMM__OTHER_MRR,
        (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        ) as check_date_rmm_others
    from
        migrated_product
        inner join max_migrated_product on migrated_product.company_id = max_migrated_product.COMPANY_ID
        and migrated_product.REPORTING_DATE = max_migrated_product.rd
    where
        ITEM_DESCRIPTION is null
    group by
        1,
        2
),

NOC as (

      SELECT
        obt.COMPANY_ID,
        obt.REPORTING_DATE,
        sum(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'NOC' and MRR_FLAG=1 and PRODUCT_CATEGORIZATION_PRODUCT_PLAN='Elite',UNITS,0)) as  NOC_SERVER_UNITS,
        sum(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'NOC' and MRR_FLAG=1 and PRODUCT_CATEGORIZATION_PRODUCT_PLAN!='Elite',UNITS,0)) as  NOC_DESKTOP_UNITS,

        sum(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'NOC' and MRR_FLAG=1 and PRODUCT_CATEGORIZATION_PRODUCT_PLAN='Elite',MRR,0)) as  NOC_SERVER_MRR,
        sum(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'NOC' and MRR_FLAG=1 and PRODUCT_CATEGORIZATION_PRODUCT_PLAN!='Elite',MRR,0)) as  NOC_DESKTOP_MRR
    FROM
        ANALYTICS.DBO.GROWTH__OBT obt
    where
        METRIC_OBJECT = 'applied_billings'
        and MRR_FLAG=1
        and REPORTING_DATE <=(
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        )
    group by
        1,
        2
),
  old_prod_1_to_cwrmm as (
      select distinct rmm.*,
                      amd.COMPANY_ID,
                      amd.REPORTING_DATE         as OLD_PROD_REPORTING_DATE,
                      OLD_PROD_UNITS,
                      OLD_PROD_MRR,
                      RMM_OTHER_UNITS,
                      RMM__OTHER_MRR,
                      rmm_other_flag,
                      OLD_PROD_FLAG,
                      rmm_others_reporting_date,
--                       *******************************************************************************************************
                      case --                 RMM is null, Automate and only RMM Other SKUs are active
                          when RMM_REPORTING_DATE is null
                              and OLD_PROD_REPORTING_DATE = check_date_rmm_others
                              and OLD_PROD_REPORTING_DATE = rmm_others_reporting_date
                              then 3 --                 RMM and RMM Others is null, Automate is active = Never moved to RMM/Still with Automate
                          when RMM_REPORTING_DATE is null
                              and rmm_others_reporting_date is null
                              and OLD_PROD_REPORTING_DATE = check_date_OLD_PROD
                              then 1 --                 RMM and RMM Other is null,  Automate data is not active date = Churned Automate
                          when RMM_REPORTING_DATE is null
                              and rmm_others_reporting_date is null
                              and OLD_PROD_REPORTING_DATE < check_date_OLD_PROD
                              then 2 --                 RMM and Automate are active = Keeping both
                          when (RMM_REPORTING_DATE = OLD_PROD_REPORTING_DATE)
                              and (RMM_REPORTING_DATE = check_date_rmm)
                              then 4 --                 RMM or RMM others reporting date is more than Automate,
                          when (OLD_PROD_REPORTING_DATE < check_date_OLD_PROD)
                              and (
                                           RMM_REPORTING_DATE = check_date_rmm
                                       or rmm_others_reporting_date = check_date_rmm_others
                                   )
                              then 5 --                 greatest date of RMM and RMM others are not active, Automate is active
                          when greatest(
                                       coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                       coalesce(rmm_others_reporting_date, '2000-01-01')
                                   ) < check_date_OLD_PROD
                              and OLD_PROD_REPORTING_DATE = check_date_OLD_PROD
                              then 6 --                 greatest date of Both RMM and RMM Others are not active and greatest date of RMMs is more than Automate
                          when (
                                      greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) > OLD_PROD_REPORTING_DATE
                                  and greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) < check_date_OLD_PROD
                              ) then 7
                          when (
                                      greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-02-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) < OLD_PROD_REPORTING_DATE
                                  and OLD_PROD_REPORTING_DATE < check_date_OLD_PROD
                                  and (
                                          coalesce(RMM_REPORTING_DATE, rmm_others_reporting_date) is not null
                                          )
                              ) then 8

                          when (
                                  greatest(
                                          coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                          coalesce(OLD_PROD_REPORTING_DATE, '2000-01-01'),
                                          coalesce(rmm_others_reporting_date, '2000-01-01')
                                      ) < check_date_OLD_PROD
                              ) then 7
                          else 9 end      as MIGRATION_CODE,
--                       ***********************************************************************************************
                      iff(
                                  OLD_PROD_REPORTING_DATE = check_date_OLD_PROD,
                                  1,
                                  0
                          )                      as ACTIVE_OLD_PROD,
                      RMM_UNITS - OLD_PROD_UNITS as units_difference,
                      RMM_MRR - OLD_PROD_MRR     as mrr_difference,
                      (RMM_MRR) * 12             as RMM_ARR,
                      pmp.PRE_MIGRATION_OLD_PROD_MRR,
                      pmp.PRE_MIGRATION_OLD_PROD_ARR,
                      pmp.PRE_MIGRATION_OLD_PROD_UNITS,
                      PRE_MIG_MAX_OLD_PROD,
                      NOC.NOC_DESKTOP_MRR,
                      NOC.NOC_DESKTOP_UNITS,
                      NOC.NOC_SERVER_MRR,
                      NOC.NOC_SERVER_UNITS,
                      'Automate to CWRMM'        as TABLE_FILTER
      from old_prod_1_max_date amd
               left join migrated_prod_agg rmm on rmm.rmm_company_id = amd.COMPANY_ID
               left join migrated_product_agg_others rmmo on amd.COMPANY_ID = rmmo.rmm_company_id
               left join old_product_1_pre_migration_price pmp
                         on amd.COMPANY_ID = pmp.COMPANY_ID and pre_migration_month_number_desc = 1
               left join NOC on NOC.COMPANY_ID = rmm.rmm_company_id and NOC.REPORTING_DATE = rmm.rmm_reporting_date
  ),

-- This is end of prod 1*******************************************************************************************************************************************
-- ****************************************************************************************************************************************************************
old_product_2 as (
    SELECT
        distinct obt.COMPANY_ID,
        obt.REPORTING_DATE,
        max(UNITS) AS OLD_PROD_UNITS,
        sum(MRR) AS OLD_PROD_MRR,
        (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        ) as CHECK_DATE_OLD_PROD
    FROM
        ANALYTICS.DBO.GROWTH__OBT obt
    WHERE
        METRIC_OBJECT = 'applied_billings'
        AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
        and REPORTING_DATE <= (
            select
                distinct case when day(CURRENT_DATE()) > 3 then date_trunc('Month', add_months(CURRENT_DATE()::date, -1)) else date_trunc('Month', add_months(CURRENT_DATE()::date, -2)) end as date
        )
        and REPORTING_DATE >= ('2021-01-01')
    GROUP BY
        1,
        2
),
old_product_2_max_date as(
--     max of Automate date
    select
        COMPANY_ID,
        max(REPORTING_DATE) as rd
    from
        old_product_2
    group by
        1
),
old_product_2_pre_migration_price as (
--     Less than min of CWRMM date price
    select
        a.COMPANY_ID,
        a.REPORTING_DATE,
        max(a.REPORTING_DATE) as PRE_MIG_MAX_OLD_PROD,
        sum(OLD_PROD_MRR) as PRE_MIGRATION_OLD_PROD_MRR,
        sum(OLD_PROD_MRR) * 12 as PRE_MIGRATION_OLD_PROD_ARR,
        sum(OLD_PROD_UNITS) as PRE_MIGRATION_OLD_PROD_UNITS,
        row_number() over (
            partition by a.COMPANY_ID
            order by
                a.REPORTING_DATE desc
        ) as pre_migration_month_number_desc,
        1 as PRE_MIGARTION_FLAG
    from
        old_product_2 a
        inner join (
            select
                distinct COMPANY_ID,
                MIN_RMM_DATE
            from
                migrated_product
        ) rmm on a.COMPANY_ID = rmm.COMPANY_ID
        and a.REPORTING_DATE < rmm.MIN_RMM_DATE
    group by
        1,
        2
    having
        sum(OLD_PROD_MRR) > 0
),
old_prod_2_max_date as (
    select
        old_product_2.*,
        1 as OLD_PROD_FLAG
    from
        old_product_2
        inner join old_product_2_max_date on old_product_2.COMPANY_ID = old_product_2_max_date.COMPANY_ID
        and old_product_2.REPORTING_DATE = old_product_2_max_date.rd
),
old_prod_2_to_cwrmm as
         (
             select distinct rmm.*,
                             amd.COMPANY_ID,
                             amd.REPORTING_DATE         as OLD_PROD_REPORTING_DATE,
                             OLD_PROD_UNITS,
                             OLD_PROD_MRR,
                             RMM_OTHER_UNITS,
                             RMM__OTHER_MRR,
                             rmm_other_flag,
                             OLD_PROD_FLAG,
                             rmm_others_reporting_date,

--                              **************************************************************************

                                 case --                 RMM is null, Automate and only RMM Other SKUs are active
                          when RMM_REPORTING_DATE is null
                              and OLD_PROD_REPORTING_DATE = check_date_rmm_others
                              and OLD_PROD_REPORTING_DATE = rmm_others_reporting_date
                              then 3 --                 RMM and RMM Others is null, Automate is active = Never moved to RMM/Still with Automate
                          when RMM_REPORTING_DATE is null
                              and rmm_others_reporting_date is null
                              and OLD_PROD_REPORTING_DATE = check_date_OLD_PROD
                              then 1 --                 RMM and RMM Other is null,  Automate data is not active date = Churned Automate
                          when RMM_REPORTING_DATE is null
                              and rmm_others_reporting_date is null
                              and OLD_PROD_REPORTING_DATE < check_date_OLD_PROD
                              then 2 --                 RMM and Automate are active = Keeping both
                          when (RMM_REPORTING_DATE = OLD_PROD_REPORTING_DATE)
                              and (RMM_REPORTING_DATE = check_date_rmm)
                              then 4 --                 RMM or RMM others reporting date is more than Automate,
                          when (OLD_PROD_REPORTING_DATE < check_date_OLD_PROD)
                              and (
                                           RMM_REPORTING_DATE = check_date_rmm
                                       or rmm_others_reporting_date = check_date_rmm_others
                                   )
                              then 5 --                 greatest date of RMM and RMM others are not active, Automate is active
                          when greatest(
                                       coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                       coalesce(rmm_others_reporting_date, '2000-01-01')
                                   ) < check_date_OLD_PROD
                              and OLD_PROD_REPORTING_DATE = check_date_OLD_PROD
                              then 6 --                 greatest date of Both RMM and RMM Others are not active and greatest date of RMMs is more than Automate
                          when (
                                      greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) > OLD_PROD_REPORTING_DATE
                                  and greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) < check_date_OLD_PROD
                              ) then 7
                          when (
                                      greatest(
                                              coalesce(RMM_REPORTING_DATE, '2000-02-01'),
                                              coalesce(rmm_others_reporting_date, '2000-01-01')
                                          ) < OLD_PROD_REPORTING_DATE
                                  and OLD_PROD_REPORTING_DATE < check_date_OLD_PROD
                                  and (
                                          coalesce(RMM_REPORTING_DATE, rmm_others_reporting_date) is not null
                                          )
                              ) then 8

                          when (
                                  greatest(
                                          coalesce(RMM_REPORTING_DATE, '2000-01-01'),
                                          coalesce(OLD_PROD_REPORTING_DATE, '2000-01-01'),
                                          coalesce(rmm_others_reporting_date, '2000-01-01')
                                      ) < check_date_OLD_PROD
                              ) then 7
                          else 9 end      as MIGRATION_CODE,

--                              *****************************************************


                             iff(
                                         OLD_PROD_REPORTING_DATE = check_date_OLD_PROD,
                                         1,
                                         0
                                 )                      as ACTIVE_OLD_PROD,
                             RMM_UNITS - OLD_PROD_UNITS as units_difference,
                             RMM_MRR - OLD_PROD_MRR     as mrr_difference,
                             (RMM_MRR) * 12             as RMM_ARR,
                             pmp.PRE_MIGRATION_OLD_PROD_MRR,
                             pmp.PRE_MIGRATION_OLD_PROD_ARR,
                             pmp.PRE_MIGRATION_OLD_PROD_UNITS,
                             PRE_MIG_MAX_OLD_PROD,
                             NOC.NOC_DESKTOP_MRR,
                             NOC.NOC_DESKTOP_UNITS,
                             NOC.NOC_SERVER_MRR,
                             NOC.NOC_SERVER_UNITS,
                             'Command to CWRMM'        as TABLE_FILTER
             from old_prod_2_max_date amd
                      left join migrated_prod_agg rmm on rmm.rmm_company_id = amd.COMPANY_ID
                      left join migrated_product_agg_others rmmo on amd.COMPANY_ID = rmmo.rmm_company_id
                      left join old_product_2_pre_migration_price pmp
                                on amd.COMPANY_ID = pmp.COMPANY_ID and pre_migration_month_number_desc = 1
                      left join NOC
                                on NOC.COMPANY_ID = rmm.rmm_company_id and NOC.REPORTING_DATE = rmm.rmm_reporting_date

         )
        --  this is end of prod 2 ***********************************************************************************************************
        -- **********************************************************************************************************************************
         select prd1.*,
                MS."automate_status" as MIGRATION_STATUS,
                current_date as run_date
from old_prod_1_to_cwrmm prd1
        left join "DATAIKU"."PRD_DATAIKU_WRITE"."CWRMM_MIGRATION_STATUS_MAPPING" MS on prd1.MIGRATION_CODE=MS."code"
union all
         select prd2.*
              ,MS."command_status" as MIGRATION_STATUS,
                current_date as run_date
from old_prod_2_to_cwrmm prd2
        left join "DATAIKU"."PRD_DATAIKU_WRITE"."CWRMM_MIGRATION_STATUS_MAPPING" MS on prd2.MIGRATION_CODE=MS."code"