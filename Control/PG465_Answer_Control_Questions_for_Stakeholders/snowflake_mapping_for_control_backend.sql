-- Project      Control Freemium Dev
-- Purpose      Used to map control partners from MS SQL Database
--              to snowflake salesforce id and snowflake company id
--
-- Input        - Tables:
--                      -- FIVETRAN.SALESFORCE.CWS_CONTROL_TRIAL_C
--                      -- CONTROL_STAGING.DBO.instance
--                      -- analytics.dbo_transformation.control_staging__screenconnect_transactions
--                      -- "CONTROL_STAGING"."DBO"."ACCOUNT"
--                      -- ANALYTICS.DBO.CORE__RPT_BILLINGS
--                      -- ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__DETAILS_PARTNERLVL
--                      -- DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
--                      -- ANALYTICS.DBO.CORE__RPT_BOOKINGS
--                              -- Pricebook details for RMM
-- Database     - Snowflake
-----------------------------------------------------------------------------------------------------------
with instance_account_id as (
    select
        accountid as ACCOUNTID,
        instanceid as INSTANCEID,
        CWS_ACCOUNT_C as ID,
        max(CREATED_DATE) as trial_created_date
    from
        CONTROL_STAGING.DBO.instance c
        left join FIVETRAN.SALESFORCE.CWS_CONTROL_TRIAL_C t on c.AccountID = t.CWS_CONTROL_ACCOUNT_ID_C
    group by
        1,
        2,
        3
),
instance_account_id_screenconnect as (
    select
        distinct --                     i.INSTANCEID,
        a.ACCOUNTID,
        Ship_to as ID
    From
        "CONTROL_STAGING"."DBO"."ACCOUNT" a
        Left Join "CONTROL_STAGING"."DBO"."INSTANCE" i on a.AccountID = i.AccountID
        Left Join analytics.dbo_transformation.control_staging__screenconnect_transactions sc on i.InstanceID = sc.InstanceID
        Left Join (
            Select
                distinct Ship_to,
                Billing_Log_Recid
            from
                ANALYTICS.DBO.CORE__RPT_BILLINGS
        ) r on r.Billing_Log_Recid = sc.OrderID
    where
        a.ACCOUNTID in (
            select
                distinct AccountID
            from
                CONTROL_STAGING.DBO.instance
        )
),
account_list as (
    select
        distinct t.ACCOUNTID,
        coalesce(t.ID, c.ID) as ID,
        INSTANCEID,
        trial_created_date
    from
        instance_account_id t
        left join instance_account_id_screenconnect c on t.ACCOUNTID = c.ACCOUNTID
),
ps as (
    select
        *,
        row_number() over(
            partition by SHIP_TO
            order by
                APPLIED_DATE desc
        ) as max_date
    from
        ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__DETAILS_PARTNERLVL
    where
        APPLIED_DATE <=(
            select
                max(reporting_date)
            from
                DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
),
final_account as (
    select
        ship_to,
        ACCOUNTID,
        sf_id,
        applied_date as last_active_date,
        companyname,
        region,
        age,
        GRP_TERR,
        ACCOUNT_OWNER,
        touch_tier,
        churn_date,
        partner_success_mgr,
        currentprods,
        currentprods_prod_billing,
        cur_portfolios,
        cur_prodgrps,
        nbrcurrprods,
        nbrcurrprods_prod_billing,
        MRR_PARTNER_LVL,
        MRR_LY_PARTNER_LVL,
        RETENTION_STATUS_MOM,
        IS_PARTNER,
        iff(
            APPLIED_DATE =(
                select
                    max(reporting_date)
                from
                    DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
            )
            and MRR_PARTNER_LVL <> 0,
            1,
            0
        ) as active_flag,
        iff(
            active_flag = 0
            and length(ship_to) > 0,
            1,
            0
        ) as past_active_flag,
        trial_created_date,
        ACV,
        OPPSTATUS,
        OPP_CLOSE_DATE,
        INSTANCEID
    from
        account_list ia
        left join ps on ia.ID = ps.SHIP_TO
        and max_date = 1
        left join (
            select
                BILL_TO,
                ACV,
                OPPSTATUS,
                CLOSEDATE as OPP_CLOSE_DATE
            from
                ANALYTICS.DBO.CORE__RPT_BOOKINGS
            where
                BU = 'Control' QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BILL_TO
                    ORDER BY
                        CLOSEDATE desc
                ) = 1
        ) b on b.BILL_TO = ps.SF_ID
)
select
    distinct max(SHIP_TO) as SHIP_TO,
    max(ACCOUNTID) as ACCOUNTID,
    INSTANCEID,
    max(SF_ID) as SF_ID,
    max(ACCOUNT_OWNER) as ACCOUNT_OWNER,
    max(REGION) as REGION,
    max(GRP_TERR) as GRP_TERR,
    max(CURRENTPRODS) CURRENTPRODS,
    max(IS_PARTNER) IS_PARTNER
from
    final_account
group by
    3