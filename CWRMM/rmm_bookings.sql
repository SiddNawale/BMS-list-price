
WITH current_partner_info_base1_a AS (
    SELECT distinct
           SHIP_TO as Automate_ship_to,
                    applied_date
    FROM ANALYTICS.DBO.CORE__RPT_BILLINGS
    WHERE YEAR(APPLIED_DATE) >= 2021
      AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
    and CYCLE_AMOUNT > 0
),
 current_partner_info_base1_a1 AS (
        SELECT distinct
           SHIP_TO as Command_ship_to,
                        applied_date
    FROM ANALYTICS.DBO.CORE__RPT_BILLINGS
    WHERE YEAR(APPLIED_DATE) >= 2021 and PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT='Command'
    and CYCLE_AMOUNT > 0
),
 current_partner_info_base1_a2 AS (
         SELECT distinct
        SHIP_TO as others_ship_to,
                         applied_date
    FROM ANALYTICS.DBO.CORE__RPT_BILLINGS
    WHERE YEAR(APPLIED_DATE) >= 2021 and PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT not in ('CW RMM','Automate','Command')
    and CYCLE_AMOUNT > 0
),
deal_info_a AS (
    SELECT
    -- feature from opportunity
            o.ID AS OPPORTUNITY_ID,
           o.ACCOUNT_ID,
           o.NAME as OPPNAME,

            0 AS IS_PARTNER, -- Is this account already a customer?
            'None' AS PREEXISTING_PRODUCTS,

           iff((cpia.Automate_ship_to) is not null ,1,0) as Autoexistflag,
           iff((cpic.Command_ship_to) is not null ,1,0) as Commandexistflag,
           iff((cpio.others_ship_to) is not null ,1,0) as Otherexistflag,

           o.TYPE,
           o.STAGE_NAME,
           o.FORECAST_CATEGORY,
           o.CREATED_DATE AS OPEN_DATE,
           o.LAST_MODIFIED_DATE as LAST_MODIFIED_DATE,
           o.CLOSE_DATE as EXPECTED_CLOSE_DATE,
        --    o.CLOSE_DATE,
           iff(o.CLOSE_DATE>TO_DATE(GETDATE()),to_Date(o.LAST_MODIFIED_DATE),to_Date(o.CLOSE_DATE)) as CLOSE_DATE,

           o.LEAD_SOURCE,
           o.IS_CLOSED,
           o.IS_WON,
           o.CWS_LOST_REASON_DETAIL_C,
           o.CWS_LOST_REASON_C,
-- feature from product
           p.PRODUCT_CODE,
           p.NAME AS PRODUCT_NAME,
           p.CWS_BRAND_NAME_C,
           p.CWS_CATEGORY_C,
-- feature from quote
           q.CWS_CONTRACT_TERM_C,
-- feature from user table
           u.USERNAME,
           u.COMPANY_NAME,
           u.CWS_TEAM_C,
           u.CWS_TEAM_GROUP_C,
           lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ', u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE))) as VLOOKUP,
           RP.VLOOKUP_VALUE,
           RP.TEAM,
           RP.GEO,
-- feature coming from quote line
           ql.SBQQ_ORIGINAL_PRICE_C,
           ql.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PU,
           ql.SBQQ_CUSTOMER_PRICE_C,
           ql.SBQQ_PARTNER_PRICE_C,
           ql.SBQQ_DISCOUNT_C,
           ql.SBQQ_QUANTITY_C,
           ql.SBQQ_NET_PRICE_C,
--features coming from contract table
        --    c.CWS_TOTAL_END_CUSTOMER_NET_PRICE_C,
        --    c.CWS_TOTAL_LIST_AMOUNT_C,
        --    c.CWS_TOTAL_NET_AMOUNT_C,
        --    c.CWS_TOTAL_REGULAR_AMOUNT_C,

--features coming from subscription table and used for Annual price, currency code and Tenure
           sb.CWS_DISTI_FACTOR_C,
           sb.CURRENCY_ISO_CODE,
           sb.CWS_ACVLINE_C, -- Price
           sb.CWS_BILLING_TERM_C,
           sb.SBQQ_SUBSCRIPTION_TYPE_C,
           sb.SBQQ_PRORATE_MULTIPLIER_C,
           sb.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PUT, -- Per Unit list price multiply tenure
           sb.SBQQ_RENEWAL_PRICE_C as SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- Per Unit Selling Price
--------------------------------------------------------
           concat(o.TYPE,o.STAGE_NAME) as filtercriteria,
           b.ACV as ACV_B,
           b.CWS_PERIOD_OF_FIXED_USAGE_COMMIT_RAMP_C as CWS_PERIOD_OF_FIXED_USAGE_COMMIT_RAMP_C,

           -- ADD IN THE ACV AMOUNT
           -- REMOVE OTHER BRAND -- THIS IS MAINLY MIN COMMIT
           -- WOULD IT HELP TO ADD AN AUTOMATE CUSTOMER COLUMN?

           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE = 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_NOC_FLAG,
           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE <> 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_HD_FLAG
    FROM ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__OPPORTUNITY o
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_C q
    ON q.ID = o.SBQQ_PRIMARY_QUOTE_C
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_LINE_C ql
    ON ql.SBQQ_QUOTE_C = q.ID

    LEFT JOIN ANALYTICS.DBO.CORE__RPT_BOOKINGS b
    ON ql.ID = b.QUOTE_LINE_ID

    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_SUBSCRIPTION_C sb
    ON ql.ID=sb.SBQQ_ORIGINAL_QUOTE_LINE_C
    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER u
    on q.OWNER_ID=u.ID
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__PRODUCT p
    ON p.ID = ql.SBQQ_PRODUCT_C

    LEFT JOIN current_partner_info_base1_a cpia
    ON cpia.Automate_ship_to = o.ACCOUNT_ID and o.CLOSE_DATE>cpia.APPLIED_DATE

    LEFT JOIN current_partner_info_base1_a1 cpic
    ON cpic.Command_ship_to = o.ACCOUNT_ID and o.CLOSE_DATE>cpic.APPLIED_DATE

    LEFT JOIN current_partner_info_base1_a2 cpio
    ON cpio.others_ship_to = o.ACCOUNT_ID and o.CLOSE_DATE>cpio.APPLIED_DATE

    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.CORE_INTERMEDIATE__REF_POSITIONS_DISTINCT RP
    on lower(RP.VLOOKUP_VALUE)=lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ',u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE)))
    WHERE o.IS_CLOSED and o.IS_DELETED=False
    AND ql.IS_DELETED = FALSE --and (o.TYPE<>'Amendment' and o.STAGE_NAME='Closed Won')
),
rmm_deals_a AS (
    SELECT DISTINCT OPPORTUNITY_ID
    FROM deal_info_a
    WHERE CWS_BRAND_NAME_C = 'RMM'
),
final_win_status_a AS (
    SELECT ACCOUNT_ID as max_won_ACCOUNT_ID,
    'Won' as "account_final_status",
    max(CLOSE_DATE) as  close_date_max_won
    FROM deal_info_a
    WHERE IS_WON = True and CWS_BRAND_NAME_C = 'RMM'
    group by 1
)
,

-- date generation

DG_a AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()), '2000-01-01 00:00:00') AS MY_DATE
    FROM TABLE (GENERATOR(ROWCOUNT =>20000))
),
DD_a as(
         SELECT TO_DATE(MY_DATE)      as basedate
              , DATE_TRUNC(MONTH,TO_DATE(MY_DATE)) AS MONTH_DATE
              , TO_TIME(MY_DATE)      as time
              , TO_TIMESTAMP(MY_DATE) as datetime
              , YEAR(MY_DATE)         as year
              , QUARTER(MY_DATE)      as quarter
              , MONTH(MY_DATE)        as month
              , MONTHNAME(MY_DATE)    as monthname
              , DAY(MY_DATE)          as day
              , DAYOFWEEK(MY_DATE)    as dayofweek
              , WEEKOFYEAR(MY_DATE)   as weekofyear
              , DAYOFYEAR(MY_DATE)    as dayofyear
              ,TO_DATE(GETDATE())      as currentdate
              ,case when WEEKOFYEAR(MY_DATE)=WEEKOFYEAR(GETDATE()) and YEAR(MY_DATE)=YEAR(GETDATE()) then concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) else '0' end as currentweek
              ,case when len(WEEKOFYEAR(MY_DATE))<2 then concat('Y',YEAR(MY_DATE),'W',0,WEEKOFYEAR(MY_DATE))
              else concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) end as YEARWEEK
         FROM DG_a
),
 final_a AS (
    SELECT di.*,fws.*, DD.YEARWEEK, DD.currentdate,DD.currentweek,
    MAX(CLOSE_DATE) OVER(PARTITION BY ACCOUNT_ID) AS MAX_CLOSE_DATE
    --, lc.EXCHANGE_RATE
    FROM deal_info_a di
    INNER JOIN rmm_deals_a rd
    ON rd.OPPORTUNITY_ID = di.OPPORTUNITY_ID
    left join DD_a DD on to_date(CLOSE_DATE)=DD.basedate
    left join final_win_status_a fws on di.ACCOUNT_ID=fws.max_won_ACCOUNT_ID
    -- where di.filtercriteria<>'AmendmentClosed Won'
    --LEFT JOIN LC lc
    --ON di.CURRENCY_ISO_CODE=lc.TRX_CURRENCY
), rmm as
    (
    SELECT       distinct
        OPPORTUNITY_ID,
        OPPNAME,
           ACCOUNT_ID,
           USERNAME,
           COMPANY_NAME,
           CWS_TEAM_C,
           CWS_TEAM_GROUP_C,
           -- VLOOKUP,
           -- VLOOKUP_VALUE,
           IS_PARTNER,
           TEAM,
           GEO,
           PREEXISTING_PRODUCTS,
           TYPE,
           STAGE_NAME,
           FORECAST_CATEGORY,
           OPEN_DATE,
           CLOSE_DATE,
           date_trunc('MONTH', "CLOSE_DATE")                       as close_date_first,
           LAST_MODIFIED_DATE,
           EXPECTED_CLOSE_DATE,
           MAX_CLOSE_DATE,
           -- CURRENTWEEK,
           case when MAX_CLOSE_DATE = CLOSE_DATE then 1 else 0 end as close_date_flag,
           LEAD_SOURCE,
           IS_CLOSED,
           IS_WON,
           PRODUCT_CODE,
           PRODUCT_NAME,
           CWS_BRAND_NAME_C,
           CWS_CATEGORY_C,
           CWS_CONTRACT_TERM_C,

           CWS_LOST_REASON_DETAIL_C,
           CWS_LOST_REASON_C,

           SBQQ_ORIGINAL_PRICE_C,
           (SBQQ_LIST_PRICE_C_PU / C."Conversion Rate")                              as SBQQ_LIST_PRICE_C_PU,
           SBQQ_CUSTOMER_PRICE_C,
           SBQQ_PARTNER_PRICE_C,
           SBQQ_DISCOUNT_C,
           SBQQ_QUANTITY_C,
           SBQQ_NET_PRICE_C,
           CWS_DISTI_FACTOR_C,
           CURRENCY_ISO_CODE,
           (CWS_ACVLINE_C / C."Conversion Rate")                                     as CWS_ACVLINE_C,
           CWS_BILLING_TERM_C,
           SBQQ_SUBSCRIPTION_TYPE_C,
           SBQQ_PRORATE_MULTIPLIER_C,

           (SBQQ_LIST_PRICE_C_PUT / C."Conversion Rate")                             as SBQQ_LIST_PRICE_C_PUT, -- list price
           (SBQQ_RENEWAL_PRICE_C_DISC_PRICE / C."Conversion Rate")                    as  SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- selling price
        //   (SBQQ_RENEWAL_PRICE_C_DISC_PRICE/(100-SBQQ_DISCOUNT_C))/ C."Conversion Rate" as tiered_per_unit_price, -- tiered price

           SOLD_WITH_NOC_FLAG,
           SOLD_WITH_HD_FLAG,
           YEARWEEK,
           currentdate,
           currentweek,
           CWS_ACVLINE_C                                           as LC_CWS_ACVLINE_C, -- local currency
           SBQQ_RENEWAL_PRICE_C_DISC_PRICE                         as LC_SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- local currency
           filtercriteria,
           C."Conversion Rate" as "Conversion Rate",
           case
               when Autoexistflag = 1 then 'Existing Automate'
               when Commandexistflag = 1 then 'Existing Command'
               when Otherexistflag = 1 then 'No Automate/Command'
               else 'New Partner' end    as partner_type,
           'RMM'                                                   as filter,
           "account_final_status",
           close_date_max_won,
           ACV_B,
            CWS_PERIOD_OF_FIXED_USAGE_COMMIT_RAMP_C
    FROM final_a
    left join DATAIKU.PRD_DATAIKU_WRITE."CW_RMM_POST_LAUNCH_CURRENCY" C on final_a.CURRENCY_ISO_CODE=C."Currency Code" AND C."Active"=1
where ((upper("account_final_status") in ('WON') and  CLOSE_DATE<=close_date_max_won) or ("account_final_status"  is null))),


-- where MAX_CLOSE_DATE=CLOSE_DATE
--   and upper(CWS_BRAND_NAME_C)<>'OTHER'

-- ================================================================= Automate========================================================================

--current products in basket
deal_info_b AS (
    SELECT
    -- feature from opportunity
            o.ID AS OPPORTUNITY_ID,
           o.ACCOUNT_ID,
                      o.NAME as OPPNAME,

           0 AS IS_PARTNER, -- Is this account already a customer?
           'None' AS PREEXISTING_PRODUCTS,
            0 as Autoexistflag,
           0 as Commandexistflag,

           o.TYPE,
           o.STAGE_NAME,
           o.FORECAST_CATEGORY,
           o.CREATED_DATE AS OPEN_DATE,
           o.LAST_MODIFIED_DATE as LAST_MODIFIED_DATE,
           o.CLOSE_DATE as EXPECTED_CLOSE_DATE,
        --    o.CLOSE_DATE,
           iff(o.CLOSE_DATE>TO_DATE(GETDATE()),to_Date(o.LAST_MODIFIED_DATE),to_Date(o.CLOSE_DATE)) as CLOSE_DATE,

           o.LEAD_SOURCE,
           o.IS_CLOSED,
           o.IS_WON,
           o.CWS_LOST_REASON_DETAIL_C,
           o.CWS_LOST_REASON_C,
-- feature from product
           p.PRODUCT_CODE,
           p.NAME AS PRODUCT_NAME,
           p.CWS_BRAND_NAME_C,
           p.CWS_CATEGORY_C,
-- feature from quote
           q.CWS_CONTRACT_TERM_C,
-- feature from user table
           u.USERNAME,
           u.COMPANY_NAME,
           u.CWS_TEAM_C,
           u.CWS_TEAM_GROUP_C,
           lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ', u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE))) as VLOOKUP,
           RP.VLOOKUP_VALUE,
           RP.TEAM,
           RP.GEO,
-- feature coming from quote line
           ql.SBQQ_ORIGINAL_PRICE_C,
           ql.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PU,
           ql.SBQQ_CUSTOMER_PRICE_C,
           ql.SBQQ_PARTNER_PRICE_C,
           ql.SBQQ_DISCOUNT_C,
           ql.SBQQ_QUANTITY_C,
           ql.SBQQ_NET_PRICE_C,

--features coming from contract table
        --    c.CWS_TOTAL_END_CUSTOMER_NET_PRICE_C,
        --    c.CWS_TOTAL_LIST_AMOUNT_C,
        --    c.CWS_TOTAL_NET_AMOUNT_C,
        --    c.CWS_TOTAL_REGULAR_AMOUNT_C,

--features coming from subscription table and used for Annual price, currency code and Tenure
           sb.CWS_DISTI_FACTOR_C,
           sb.CURRENCY_ISO_CODE,
           sb.CWS_ACVLINE_C, -- Price
           sb.CWS_BILLING_TERM_C,
           sb.SBQQ_SUBSCRIPTION_TYPE_C,
           sb.SBQQ_PRORATE_MULTIPLIER_C,
           sb.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PUT, -- Per Unit list price multiply tenure
           sb.SBQQ_RENEWAL_PRICE_C as SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- Per Unit Selling Price
--------------------------------------------------------
           concat(o.TYPE,o.STAGE_NAME) as filtercriteria,
           -- ADD IN THE ACV AMOUNT
           -- REMOVE OTHER BRAND -- THIS IS MAINLY MIN COMMIT
           -- WOULD IT HELP TO ADD AN AUTOMATE CUSTOMER COLUMN?

           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE = 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_NOC_FLAG,
           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE <> 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_HD_FLAG
    FROM ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__OPPORTUNITY o
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_C q
    ON q.ID = o.SBQQ_PRIMARY_QUOTE_C
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_LINE_C ql
    ON ql.SBQQ_QUOTE_C = q.ID
    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_SUBSCRIPTION_C sb
    ON ql.ID=sb.SBQQ_ORIGINAL_QUOTE_LINE_C
    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER u
    on q.OWNER_ID=u.ID
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__PRODUCT p
    ON p.ID = ql.SBQQ_PRODUCT_C
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.CORE_INTERMEDIATE__REF_POSITIONS_DISTINCT RP
    on lower(RP.VLOOKUP_VALUE)=lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ',u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE)))
    WHERE o.IS_CLOSED
      and o.IS_DELETED=False
    AND ql.IS_DELETED = FALSE --and (o.TYPE<>'Amendment' and o.STAGE_NAME='Closed Won')
),
rmm_deals_b AS (
    SELECT DISTINCT OPPORTUNITY_ID
    FROM deal_info_b
    WHERE CWS_BRAND_NAME_C = 'Automate'
),

final_win_status_b AS (
    SELECT ACCOUNT_ID as max_won_ACCOUNT_ID,
    'Won' as "account_final_status",
    max(CLOSE_DATE) as  close_date_max_won
    FROM deal_info_b
    WHERE IS_WON = True and CWS_BRAND_NAME_C = 'Automate'
    group by 1
),

-- date generation

DG_b AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()), '2000-01-01 00:00:00') AS MY_DATE
    FROM TABLE (GENERATOR(ROWCOUNT =>20000))
),
DD_b as(
         SELECT TO_DATE(MY_DATE)      as basedate
              , DATE_TRUNC(MONTH,TO_DATE(MY_DATE)) AS MONTH_DATE
              , TO_TIME(MY_DATE)      as time
              , TO_TIMESTAMP(MY_DATE) as datetime
              , YEAR(MY_DATE)         as year
              , QUARTER(MY_DATE)      as quarter
              , MONTH(MY_DATE)        as month
              , MONTHNAME(MY_DATE)    as monthname
              , DAY(MY_DATE)          as day
              , DAYOFWEEK(MY_DATE)    as dayofweek
              , WEEKOFYEAR(MY_DATE)   as weekofyear
              , DAYOFYEAR(MY_DATE)    as dayofyear
              ,TO_DATE(GETDATE())      as currentdate
              ,case when WEEKOFYEAR(MY_DATE)=WEEKOFYEAR(GETDATE()) and YEAR(MY_DATE)=YEAR(GETDATE()) then concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) else '0' end as currentweek
              ,case when len(WEEKOFYEAR(MY_DATE))<2 then concat('Y',YEAR(MY_DATE),'W',0,WEEKOFYEAR(MY_DATE))
              else concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) end as YEARWEEK
         FROM DG_b
),
 final_b AS (
    SELECT di.*,fws.*,DD.YEARWEEK, DD.currentdate,DD.currentweek,
    MAX(CLOSE_DATE) OVER(PARTITION BY ACCOUNT_ID) AS MAX_CLOSE_DATE
    --, lc.EXCHANGE_RATE
    FROM deal_info_b di
    INNER JOIN rmm_deals_b rd
    ON rd.OPPORTUNITY_ID = di.OPPORTUNITY_ID
    left join DD_b DD on to_date(CLOSE_DATE)=DD.basedate
    left join final_win_status_b fws on di.ACCOUNT_ID=fws.max_won_ACCOUNT_ID
    -- where di.filtercriteria<>'AmendmentClosed Won'
    --LEFT JOIN LC lc
    --ON di.CURRENCY_ISO_CODE=lc.TRX_CURRENCY
),
  automate as (
SELECT       distinct
                      OPPORTUNITY_ID,
                        OPPNAME,

                      ACCOUNT_ID,
           USERNAME,
           COMPANY_NAME,
           CWS_TEAM_C,
           CWS_TEAM_GROUP_C,
           -- VLOOKUP,
           -- VLOOKUP_VALUE,
           IS_PARTNER,
           TEAM,
           GEO,
           PREEXISTING_PRODUCTS,
           TYPE,
           STAGE_NAME,
           FORECAST_CATEGORY,
           OPEN_DATE,
           CLOSE_DATE,

           date_trunc('MONTH', "CLOSE_DATE")                       as close_date_first,
           LAST_MODIFIED_DATE,
           EXPECTED_CLOSE_DATE,
           MAX_CLOSE_DATE,
           -- CURRENTWEEK,
           case when MAX_CLOSE_DATE = CLOSE_DATE then 1 else 0 end as close_date_flag,
           LEAD_SOURCE,
           IS_CLOSED,
           IS_WON,
           PRODUCT_CODE,
           PRODUCT_NAME,
           CWS_BRAND_NAME_C,
           CWS_CATEGORY_C,
           CWS_CONTRACT_TERM_C,

           CWS_LOST_REASON_DETAIL_C,
           CWS_LOST_REASON_C,

           SBQQ_ORIGINAL_PRICE_C,
           (SBQQ_LIST_PRICE_C_PU / C."Conversion Rate")                              as SBQQ_LIST_PRICE_C_PU,
           SBQQ_CUSTOMER_PRICE_C,
           SBQQ_PARTNER_PRICE_C,
           SBQQ_DISCOUNT_C,
           SBQQ_QUANTITY_C,
           SBQQ_NET_PRICE_C,
           CWS_DISTI_FACTOR_C,
           CURRENCY_ISO_CODE,
           (CWS_ACVLINE_C / C."Conversion Rate")                                     as CWS_ACVLINE_C,
           CWS_BILLING_TERM_C,
           SBQQ_SUBSCRIPTION_TYPE_C,
           SBQQ_PRORATE_MULTIPLIER_C,
           (SBQQ_LIST_PRICE_C_PUT / C."Conversion Rate")                             as SBQQ_LIST_PRICE_C_PUT, -- list price
           (SBQQ_RENEWAL_PRICE_C_DISC_PRICE / C."Conversion Rate")                    as  SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- selling price
           //(SBQQ_RENEWAL_PRICE_C_DISC_PRICE/(100-SBQQ_DISCOUNT_C))/ C."Conversion Rate" as tiered_per_unit_price, -- tiered price

           SOLD_WITH_NOC_FLAG,
           SOLD_WITH_HD_FLAG,
           YEARWEEK,
           currentdate,
           currentweek,

           -- CWS_TOTAL_END_CUSTOMER_NET_PRICE_C,
           -- CWS_TOTAL_LIST_AMOUNT_C,
           -- CWS_TOTAL_NET_AMOUNT_C,
           -- CWS_TOTAL_REGULAR_AMOUNT_C,

           -- EXCHANGE_RATE,
           CWS_ACVLINE_C                                           as LC_CWS_ACVLINE_C,
           SBQQ_RENEWAL_PRICE_C_DISC_PRICE                         as LC_SBQQ_RENEWAL_PRICE_C_DISC_PRICE,
           filtercriteria,
           C."Conversion Rate" as "Conversion Rate",
           case
               when Autoexistflag = 1 then 'Existing Automate'
               when Commandexistflag = 1 then 'Existing Command'
               when IS_PARTNER = True then 'Existing no automate'
               when IS_PARTNER = False then 'New Partner'
               else 'Others' end                                   as partner_type,
           'Automate'                                                   as filter,
         "account_final_status",
           close_date_max_won,
                      0 as ACV_B,
                      0 as CWS_PERIOD_OF_FIXED_USAGE_COMMIT_RAMP_C
    FROM final_b
    left join DATAIKU.PRD_DATAIKU_WRITE."CW_RMM_POST_LAUNCH_CURRENCY" C on final_b.CURRENCY_ISO_CODE=C."Currency Code"  AND C."Active"=1
      where (upper("account_final_status") in ('WON') and  CLOSE_DATE<=close_date_max_won) or ("account_final_status"  is null)
  ),
-- ================================================================= Command========================================================================
--current products in basket
deal_info_c AS (
    SELECT
    -- feature from opportunity
            o.ID AS OPPORTUNITY_ID,
                      o.NAME as OPPNAME,
           o.ACCOUNT_ID,
           0 AS IS_PARTNER, -- Is this account already a customer?
           'None' AS PREEXISTING_PRODUCTS,
            0 as Autoexistflag,
           0 as Commandexistflag,

           o.TYPE,
           o.STAGE_NAME,
           o.FORECAST_CATEGORY,
           o.CREATED_DATE AS OPEN_DATE,
           o.LAST_MODIFIED_DATE as LAST_MODIFIED_DATE,
           o.CLOSE_DATE as EXPECTED_CLOSE_DATE,
        --    o.CLOSE_DATE,
           iff(o.CLOSE_DATE>TO_DATE(GETDATE()),to_Date(o.LAST_MODIFIED_DATE),to_Date(o.CLOSE_DATE)) as CLOSE_DATE,

           o.LEAD_SOURCE,
           o.IS_CLOSED,
           o.IS_WON,
           o.CWS_LOST_REASON_DETAIL_C,
           o.CWS_LOST_REASON_C,
-- feature from product
           p.PRODUCT_CODE,
           p.NAME AS PRODUCT_NAME,
           p.CWS_BRAND_NAME_C,
           p.CWS_CATEGORY_C,
-- feature from quote
           q.CWS_CONTRACT_TERM_C,
-- feature from user table
           u.USERNAME,
           u.COMPANY_NAME,
           u.CWS_TEAM_C,
           u.CWS_TEAM_GROUP_C,
           lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ', u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE))) as VLOOKUP,
           RP.VLOOKUP_VALUE,
           RP.TEAM,
           RP.GEO,
-- feature coming from quote line
           ql.SBQQ_ORIGINAL_PRICE_C,
           ql.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PU,
           ql.SBQQ_CUSTOMER_PRICE_C,
           ql.SBQQ_PARTNER_PRICE_C,
           ql.SBQQ_DISCOUNT_C,
           ql.SBQQ_QUANTITY_C,
           ql.SBQQ_NET_PRICE_C,

--features coming from contract table
        --    c.CWS_TOTAL_END_CUSTOMER_NET_PRICE_C,
        --    c.CWS_TOTAL_LIST_AMOUNT_C,
        --    c.CWS_TOTAL_NET_AMOUNT_C,
        --    c.CWS_TOTAL_REGULAR_AMOUNT_C,

--features coming from subscription table and used for Annual price, currency code and Tenure
           sb.CWS_DISTI_FACTOR_C,
           sb.CURRENCY_ISO_CODE,
           sb.CWS_ACVLINE_C, -- Price
           sb.CWS_BILLING_TERM_C,
           sb.SBQQ_SUBSCRIPTION_TYPE_C,
           sb.SBQQ_PRORATE_MULTIPLIER_C,
           sb.SBQQ_LIST_PRICE_C as SBQQ_LIST_PRICE_C_PUT, -- Per Unit list price multiply tenure
           sb.SBQQ_RENEWAL_PRICE_C as SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- Per Unit Selling Price
--------------------------------------------------------
           concat(o.TYPE,o.STAGE_NAME) as filtercriteria,
           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE = 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_NOC_FLAG,
           MAX( IFF(p.CWS_CATEGORY_C = 'Assist - HD' AND p.PRODUCT_CODE <> 'RMMASSISTNOCSRVELITE', 1, 0) ) OVER ( PARTITION BY OPPORTUNITY_ID ) AS SOLD_WITH_HD_FLAG
    FROM ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__OPPORTUNITY o
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_C q
    ON q.ID = o.SBQQ_PRIMARY_QUOTE_C
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_QUOTE_LINE_C ql
    ON ql.SBQQ_QUOTE_C = q.ID
    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__SBQQ_SUBSCRIPTION_C sb
    ON ql.ID=sb.SBQQ_ORIGINAL_QUOTE_LINE_C
    left join ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER u
    on q.OWNER_ID=u.ID
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__PRODUCT p
    ON p.ID = ql.SBQQ_PRODUCT_C
    LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.CORE_INTERMEDIATE__REF_POSITIONS_DISTINCT RP
    on lower(RP.VLOOKUP_VALUE)=lower(CONCAT(CONCAT(LEFT(u.NAME, 1), SUBSTRING(u.NAME, CHARINDEX(' ',u.NAME) + 1, len(u.NAME))), '-', YEAR(o.CLOSE_DATE), '-', DATE_PART(quarter, o.CLOSE_DATE), '-', DATE_PART(MONTH, o.CLOSE_DATE)))
    WHERE o.IS_CLOSED and o.IS_DELETED=False
    AND ql.IS_DELETED = FALSE --and (o.TYPE<>'Amendment' and o.STAGE_NAME='Closed Won')
),
rmm_deals_c AS (
    SELECT DISTINCT OPPORTUNITY_ID
    FROM deal_info_c
    WHERE CWS_BRAND_NAME_C = 'Command'
),

-- date generation

DG_c AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()), '2000-01-01 00:00:00') AS MY_DATE
    FROM TABLE (GENERATOR(ROWCOUNT =>20000))
),
DD_c as(
         SELECT TO_DATE(MY_DATE)      as basedate
              , DATE_TRUNC(MONTH,TO_DATE(MY_DATE)) AS MONTH_DATE
              , TO_TIME(MY_DATE)      as time
              , TO_TIMESTAMP(MY_DATE) as datetime
              , YEAR(MY_DATE)         as year
              , QUARTER(MY_DATE)      as quarter
              , MONTH(MY_DATE)        as month
              , MONTHNAME(MY_DATE)    as monthname
              , DAY(MY_DATE)          as day
              , DAYOFWEEK(MY_DATE)    as dayofweek
              , WEEKOFYEAR(MY_DATE)   as weekofyear
              , DAYOFYEAR(MY_DATE)    as dayofyear
              ,TO_DATE(GETDATE())      as currentdate
              ,case when WEEKOFYEAR(MY_DATE)=WEEKOFYEAR(GETDATE()) and YEAR(MY_DATE)=YEAR(GETDATE()) then concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) else '0' end as currentweek
              ,case when len(WEEKOFYEAR(MY_DATE))<2 then concat('Y',YEAR(MY_DATE),'W',0,WEEKOFYEAR(MY_DATE))
              else concat('Y',YEAR(MY_DATE),'W',WEEKOFYEAR(MY_DATE)) end as YEARWEEK
         FROM DG_c
),

final_win_status_c AS (
    SELECT ACCOUNT_ID as max_won_ACCOUNT_ID,
    'Won' as "account_final_status",
    max(CLOSE_DATE) as  close_date_max_won
    FROM deal_info_c
     WHERE IS_WON = True and CWS_BRAND_NAME_C = 'Command'
    group by 1
),
 final_c AS (
    SELECT di.*,fws.*,DD.YEARWEEK, DD.currentdate,DD.currentweek,
    MAX(CLOSE_DATE) OVER(PARTITION BY ACCOUNT_ID) AS MAX_CLOSE_DATE
    --, lc.EXCHANGE_RATE
    FROM deal_info_c di
    INNER JOIN rmm_deals_c rd
    ON rd.OPPORTUNITY_ID = di.OPPORTUNITY_ID
    left join DD_c DD on to_date(CLOSE_DATE)=DD.basedate
     left join final_win_status_c fws on di.ACCOUNT_ID=fws.max_won_ACCOUNT_ID
    -- where di.filtercriteria<>'AmendmentClosed Won'
    --LEFT JOIN LC lc
    --ON di.CURRENCY_ISO_CODE=lc.TRX_CURRENCY
),
Command as (
SELECT
       distinct
       OPPORTUNITY_ID,
        OPPNAME,

           ACCOUNT_ID,
           USERNAME,
           COMPANY_NAME,
           CWS_TEAM_C,
           CWS_TEAM_GROUP_C,
           -- VLOOKUP,
           -- VLOOKUP_VALUE,
           IS_PARTNER,
           TEAM,
           GEO,
           PREEXISTING_PRODUCTS,
           TYPE,
           STAGE_NAME,
           FORECAST_CATEGORY,
           OPEN_DATE,
           CLOSE_DATE,

           date_trunc('MONTH', "CLOSE_DATE")                       as close_date_first,
           LAST_MODIFIED_DATE,
           EXPECTED_CLOSE_DATE,
           MAX_CLOSE_DATE,
           -- CURRENTWEEK,
           case when MAX_CLOSE_DATE = CLOSE_DATE then 1 else 0 end as close_date_flag,
           LEAD_SOURCE,
           IS_CLOSED,
           IS_WON,
           PRODUCT_CODE,
           PRODUCT_NAME,
           CWS_BRAND_NAME_C,
           CWS_CATEGORY_C,
           CWS_CONTRACT_TERM_C,

           CWS_LOST_REASON_DETAIL_C,
           CWS_LOST_REASON_C,

           SBQQ_ORIGINAL_PRICE_C,
           (SBQQ_LIST_PRICE_C_PU / C."Conversion Rate")                              as SBQQ_LIST_PRICE_C_PU,
           SBQQ_CUSTOMER_PRICE_C,
           SBQQ_PARTNER_PRICE_C,
           SBQQ_DISCOUNT_C,
           SBQQ_QUANTITY_C,
           SBQQ_NET_PRICE_C,
           CWS_DISTI_FACTOR_C,
           CURRENCY_ISO_CODE,
           (CWS_ACVLINE_C / C."Conversion Rate")                                     as CWS_ACVLINE_C,                   -- removed multiply local currency exchange to use flat file
           CWS_BILLING_TERM_C,
           SBQQ_SUBSCRIPTION_TYPE_C,
           SBQQ_PRORATE_MULTIPLIER_C,

           (SBQQ_LIST_PRICE_C_PUT / C."Conversion Rate")                             as SBQQ_LIST_PRICE_C_PUT, -- list price
           (SBQQ_RENEWAL_PRICE_C_DISC_PRICE / C."Conversion Rate")                    as  SBQQ_RENEWAL_PRICE_C_DISC_PRICE, -- selling price
         //  (SBQQ_RENEWAL_PRICE_C_DISC_PRICE/(100-SBQQ_DISCOUNT_C))/ C."Conversion Rate" as tiered_per_unit_price, -- tiered price

           SOLD_WITH_NOC_FLAG,
           SOLD_WITH_HD_FLAG,
           YEARWEEK,
           currentdate,
           currentweek,
           CWS_ACVLINE_C                                           as LC_CWS_ACVLINE_C,
           SBQQ_RENEWAL_PRICE_C_DISC_PRICE                         as LC_SBQQ_RENEWAL_PRICE_C_DISC_PRICE,
           filtercriteria,
           C."Conversion Rate" as "Conversion Rate",
           case
               when Autoexistflag = 1 then 'Existing Automate'
               when Commandexistflag = 1 then 'Existing Command'
               when IS_PARTNER = True then 'Existing no automate'
               when IS_PARTNER = False then 'New Partner'
               else 'Others' end                                   as partner_type,
           'Command'                                                   as filter,
         "account_final_status",
           close_date_max_won,
                       0 as ACV_B,
                       0 as CWS_PERIOD_OF_FIXED_USAGE_COMMIT_RAMP_C
    FROM final_c
    left join DATAIKU.PRD_DATAIKU_WRITE."CW_RMM_POST_LAUNCH_CURRENCY" C on final_c.CURRENCY_ISO_CODE=C."Currency Code"  AND C."Active"=1
          where (upper("account_final_status") in ('WON') and  CLOSE_DATE<=close_date_max_won) or ("account_final_status"  is null)
    )
select * from rmm where (IS_WON=True and CWS_ACVLINE_C is not null) or IS_WON=False
union all select * from automate where (IS_WON=True and CWS_ACVLINE_C is not null) or IS_WON=False
union all  select * from command where (IS_WON=True and CWS_ACVLINE_C is not null) or IS_WON=False