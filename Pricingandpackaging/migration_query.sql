-- Author       Product Growth
-- Created      Early 2022 and continually updated
-- Project      Pricing and packaging, RMM / BMS migration
-- Purpose      Computes required fields, metrics and measures to create a cost-comparison
--              between products/packages partners have today and how futur elooks like under
--              new packages/bundles.
--
-- Input        - Tables:
--                      -- ANALYTICS.DBO.GROWTH_OBT (could be replaced by CORE_RPT_BILLINGS)
--                              -- units, MRR, ARR, activeflags etc.
--                      -- ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__HEALTHSCORES
--                              --- HEALTHSCORE, HEALTHSCORE_ALPHA
--                      -- ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__TECH_TOUCH_ROSTER
--                              --- TOUCH_TIER, TT max/min etc.
--                      -- ANALYTICS.dbo.CORE__RPT_BILLINGS
--                              --- CONTRACT_TYPE
--                      -- DATAIKU.ENV.automate_and_manage_cal (A.H. combine it with this query )
--                              --- Calculated Manage/Automate-specific quantities. E.g. PSA_LEGACY_ON_PREM
--                      -- DATAIKU.ENV.PNP_DASHBOARD_ARR_AND_BILLING_C
--                              --- current_monthly CTE, current_monthly_rmm CTE
--                      --  DATAIKU.ENV.PNP_DASHBOARD_BUSINESS_MANAGEMENT_PRICEBOOK_STAGING
--                              --- Pricebook details for BMS (A.H. should unify it with RMM)
--                      -- DATAIKU.ENV.PNP_DASHBOARD_RMM_PRICEBOOK_STAGING
--                              -- Pricebook details for RMM
--                      --DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
--                              -- Reporting date limit
-- Output       - DATAIKU.ENV.pnp_dashboard_migration_query_c
--
-- Steps
--  base CTE, Intermediate 1 to 4: Pull Automate and Manage data to create Cloud/On Prem and Legacy flags
--  Intermediate 5      : Calculate units and mrr based on include/exclude SKU file
-- automate_manage_calc : Calculate Cloud/On Prem and Legacy units/mrr of Automate and Manage
-- Using numbers and flags from intermediate 1-5
-- - customer_roster CTE: Base query to compute
--                      -- Main flags for product usage and active partners
--                      -- Main metrics : units (seats), MRR
--              - contract CTE:
--                      -- Get contract end dates per partner and product
--                      -- Set the "earliest date" as closest date a contract is up for renewal
--              - customer_healthscores CTE:
--                      -- Partner health score
--              - customer_touch_tier CTE:
--                      -- Partner touch tier
--              - customer_contract_type CTE:
--                      -- CONTRACT_TYPE
--              - customer_psa_package CTE: -- A.H. : Do we need this or can we just set it from SKU packahe / plan  ?
--                      -- PSA_PACKAGE
--              -- customer_tenure CTE:
--                      -- Start date / tenure across main product groups
--              -- current_monthly CTE:
--                      -- "Seat Type", "Current Monthly Total"
--              -- monthly_price_cmp:
--                      -- "Current Monthly Total", cmp
--              -- current_monthly_rmm CTE:
--                      -- "Seat Type", "Current Monthly Total RMM"
--              -- monthly_price_cmp_rmm CTE:
--                      -- "Current Monthly Total RMM", cmp_rmm
--
--              -- Final select :
--                      -- Combine all fields
--                      -- Compute migration logic for RMM / BMS Packages
--                      -- Bringing in pricebook numbers
--                      -- Compute pricing & Packaging metrics for migration
--                      -- Bringing in currency conversion rates
--
--
--
-- Pending      - As of March, 2023:
--                      -- More accurate definition of "active partner" for each product
--                      -- Add additional fields in the SKP query ?
--                      -- Streamline the pricebook load process
--                      -- Parametrize the output tables
--                      -- Unify definitions with other queries (SKP model, dashboard queries etc.)
--                      -- Merge intermediate queries (e.g. Manage_Automate_calc query)
--                      '🙈 nothing to see here' is not used anymore. It is replaced by "NA-dbt_value"
-------------------------------------------------------------------------------
WITH arr_billings as (
    select
        *,
        iff(
            product in ('Manage', 'Sell', 'BrightGauge'),
            1,
            0
        ) as MSB_FLAG
    from
        DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_ARR_AND_BILLING_C
    where
        REPORTING_DATE = (
            select
                distinct REPORTING_DATE
            from
                DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
),
growth_obt as (
    select
        *
    FROM
        ANALYTICS.DBO.GROWTH__OBT
    WHERE
        REPORTING_DATE = (
            select
                distinct REPORTING_DATE
            from
                DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
        and METRIC_OBJECT = 'applied_billings'
),
base as (
    SELECT
        REPORTING_DATE,
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        PRODUCT_CATEGORIZATION_PRODUCT_LINE,
        PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE,
        ITEM_DESCRIPTION,
        PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE,
        max(
            IFF(
                (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                )
                and (
                    upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%MAINTENANCE%'
                    or upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%ASSURANCE%'
                ),
                1,
                0
            )
        ) as Base_Manage_Legacy_On_Prem,
        max(
            IFF(
                (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                )
                and (
                    upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SUBSCRIPTION%'
                ),
                1,
                0
            )
        ) as Base_Manage_On_Prem,
        max(
            IFF(
                (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                )
                and (
                    upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SAAS%'
                ),
                1,
                0
            )
        ) as Base_Manage_Cloud,
        max(
            IFF(
                (
                    (
                        PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                        AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                    )
                    and (
                        upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%MAINTENANCE%'
                        or upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%ASSURANCE%'
                    )
                ),
                1,
                0
            )
        ) as Base_Automate_Legacy_On_Prem,
        max(
            IFF(
                (
                    (
                        PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                        AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                    )
                    and (
                        upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SUBSCRIPTION%'
                    )
                ),
                1,
                0
            )
        ) as Base_Automate_On_Prem,
        max(
            IFF(
                (
                    (
                        PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                        AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                    )
                    and (
                        upper(PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE) like '%SAAS%'
                    )
                ),
                1,
                0
            )
        ) as Base_Automate_Cloud
    FROM
        growth_obt
    WHERE
        Company_name <> ''
        and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Manage', 'Automate', 'Command')
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
    HAVING
        SUM(BILLINGS) > 0
),
intermediate1 as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        case
            when Base_Manage_Legacy_On_Prem = 1 then 1
            when Base_Manage_On_Prem = 1 then 2
            when Base_Manage_Cloud = 1 then 3
        end as Manage_Category,
        case
            when Base_Automate_Legacy_On_Prem = 1 then 1
            when Base_Automate_On_Prem = 1 then 2
            when Base_Automate_Cloud = 1 then 3
        end as Automate_Category
    from
        base
),
intermediate2 as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        min(Manage_Category) as Manage_Category,
        min(Automate_Category) as Automate_Category
    from
        intermediate1
    group by
        1,
        2
),
intermediate3 as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
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
    from
        intermediate2
),
intermediate4 as (
    select
        distinct COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        Manage_Category,
        Automate_Category
    from
        intermediate3
),
intermediate5 as (
    select
        distinct intermediate4.company_id,
        intermediate4.COMPANY_NAME_WITH_ID,
        manage_category,
        automate_category,
        iff(
            Manage_Category <> 'NA'
            and Manage_Category = 'Legacy_On_Prem',
            1,
            0
        ) as PSA_LEGACY_ON_PREM,
        iff(
            Manage_Category <> 'NA'
            and Manage_Category = 'On_Prem',
            1,
            0
        ) as PSA_ON_PREM,
        iff(
            Manage_Category <> 'NA'
            and Manage_Category = 'Cloud',
            1,
            0
        ) as PSA_CLOUD,
        iff(
            Automate_Category <> 'NA'
            and Automate_Category = 'Legacy_On_Prem',
            1,
            0
        ) as AUTOMATE_LEGACY_ON_PREM,
        iff(
            Automate_Category <> 'NA'
            and Automate_Category = 'On_Prem',
            1,
            0
        ) as AUTOMATE_ON_PREM,
        iff(
            Automate_Category <> 'NA'
            and Automate_Category = 'Cloud',
            1,
            0
        ) as AUTOMATE_CLOUD,
        REPORTING_DATE,
        --expanded columns and included the arr table columns
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
    from
        intermediate4
        left join arr_billings arr -- merged queries step 1
        on intermediate4.COMPANY_ID = arr.COMPANY_ID
),
intermediate6 as (
    select
        company_id,
        COMPANY_NAME_WITH_ID,
        manage_category,
        automate_category,
        psa_legacy_on_prem,
        psa_on_prem,
        psa_cloud,
        automate_legacy_on_prem,
        automate_on_prem,
        automate_cloud,
        SUM(
            iff(
                "Brand" = 'Manage'
                and "Seat Type" = 'Include in Current ARR calculation',
                arr,
                0
            )
        ) as PSA_ARR,
        --psa arr
        sum(
            iff(
                "Brand" = 'Automate'
                or PACKAGE = 'Desktops'
                or PACKAGE = 'Networks'
                or PACKAGE = 'Servers',
                ARR,
                0
            )
        ) as AUTOMATE_ARR,
        --automate arr
        sum(
            iff(
                "Brand" = 'Manage'
                and UNITS_INCLUDE_FLAG = 'Include in Unit Count',
                UNITS,
                0
            )
        ) as PSA_UNITS,
        --psa units
        sum(
            iff(
                "Brand" = 'Automate'
                or PACKAGE = 'Desktops'
                or PACKAGE = 'Networks'
                or PACKAGE = 'Servers',
                UNITS,
                0
            )
        ) as AUTOMATE_UNITS,
        --automate units
        sum(iff("Brand" = 'Automate', UNITS, 0)) as AUTOMATE_ONLY_UNITS
    from
        intermediate5
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
automate_manage_calc as (
    select
        *,
        iff(Manage_Category = 'On_Prem', PSA_ARR, 0) as PSA_ON_PREM_ARR,
        iff(Manage_Category = 'Cloud', PSA_ARR, 0) as PSA_CLOUD_ARR,
        iff(Manage_Category = 'Legacy_On_Prem', PSA_ARR, 0) as PSA_LEGACY_ON_PREM_ARR,
        iff(Manage_Category = 'On_Prem', PSA_UNITS, 0) as PSA_ON_PREM_UNITS,
        iff(Manage_Category = 'Cloud', PSA_UNITS, 0) as PSA_CLOUD_UNITS,
        iff(Manage_Category = 'Legacy_On_Prem', PSA_UNITS, 0) as PSA_LEGACY_ON_PREM_UNITS,
        iff(Automate_Category = 'Cloud', AUTOMATE_ARR, 0) as AUTOMATE_CLOUD_ARR,
        iff(Automate_Category = 'On_Prem', AUTOMATE_ARR, 0) as AUTOMATE_ON_PREM_ARR,
        iff(
            Automate_Category = 'Legacy_On_Prem',
            AUTOMATE_ARR,
            0
        ) as AUTOMATE_LEGACY_ON_PREM_ARR,
        iff(Automate_Category = 'Cloud', AUTOMATE_UNITS, 0) as AUTOMATE_CLOUD_UNITS,
        iff(Automate_Category = 'On_Prem', AUTOMATE_UNITS, 0) as AUTOMATE_ON_PREM_UNITS,
        iff(
            Automate_Category = 'Legacy_On_Prem',
            AUTOMATE_UNITS,
            0
        ) as AUTOMATE_LEGACY_ON_PREM_UNITS
    from
        intermediate6
),
customer_roster AS (
    SELECT
        REPORTING_DATE,
        COMPANY_NAME_WITH_ID,
        rtrim(ltrim(COMPANY_ID)) as COMPANY_ID,
        --trimmed companyy id
        min(
            iff(
                COMPANY_NAME = '🙈 nothing to see here',
                null,
                COMPANY_NAME
            )
        ) as COMPANY_NAME,
        --replaced nothing to see here with null
        -----------------------------------------------------------------
        -- Set active partner flag for each products
        -- TBD: need more accurate definition for some products
        -----------------------------------------------------------------
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS manage_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Control'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS control_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS automate_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS sell_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Fortify'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS fortify_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS command_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS brightgauge_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Recover'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS recover_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS help_desk_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS itboost_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Perch'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS security_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'IT Nation'
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS itnation_active_partner,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'IT Nation'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Evolve', 'ITN Evolve')
                AND BILLINGS > 0,
                1,
                0
            )
        ) AS itnation_peer_group_active_partner,
        MAX(
            IFF(
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID <> '3P-SAAS3002315EPPRMM',
                1,
                0
            )
        ) AS webroot_active_partner,
        -----------------------------------------------------------------
        -- RMM (Automate / Command) units
        -- TBD: need more accurate definition for some products
        -----------------------------------------------------------------
        --            SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
        --                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers'), UNITS,
        --                    0))                                                                   AS RMM_UNITS,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
                and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Automate'
                and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-', 'Standard', 'Internal IT')
                and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in (
                    'SaaS',
                    'Maintenance',
                    'On Premise (Subscription)'
                ),
                UNITS,
                0
            )
        ) AS AUTOMATE_UNITS,
        ------------------------------------------------------------
        -- Command:
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers', 'Desktops'),
                UNITS,
                0
            )
        ) + sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ilike ('%command%'),
                UNITS,
                0
            )
        ) as COMMAND_TOTAL_UNITS,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops'),
                UNITS,
                0
            )
        ) AS COMMAND_DESKTOP_UNITS,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Networks'),
                UNITS,
                0
            )
        ) AS COMMAND_NETWORK_UNITS,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Servers'),
                UNITS,
                0
            )
        ) AS COMMAND_SERVER_UNITS,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Desktops')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Essential', 'M2M Essential'),
                UNITS,
                0
            )
        ) as COMMAND_ESSENTIAL_DESKTOP_UNITS,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Essential', 'M2M Essential'),
                UNITS,
                0
            )
        ) as COMMAND_ESSENTIAL_SERVER_UNITS,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Elite', 'M2M Elite'),
                UNITS,
                0
            )
        ) as COMMAND_ELITE_SERVER_UNITS,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Preferred', 'M2M Preferred'),
                UNITS,
                0
            )
        ) as COMMAND_PREFERRED_SERVER_UNITS,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ilike ('%command%'),
                UNITS,
                0
            )
        ) as COMMAND_HELPDESK_UNITS,
        -----------------------------------------------------------------
        -- Command MRR
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers', 'Desktops', 'Networks'),
                MRR,
                0
            )
        ) as COMMAND_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ilike ('%command%'),
                MRR,
                0
            )
        ) as COMMAND_HD_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Desktops'),
                MRR,
                0
            )
        ) as COMMAND_DESKTOP_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers'),
                MRR,
                0
            )
        ) as COMMAND_SERVER_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Desktops')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Essential', 'M2M Essential'),
                MRR,
                0
            )
        ) as COMMAND_ESSENTIAL_DESKTOP_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Essential', 'M2M Essential'),
                MRR,
                0
            )
        ) as COMMAND_ESSENTIAL_SERVER_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Elite', 'M2M Elite'),
                MRR,
                0
            )
        ) as COMMAND_ELITE_SERVER_MRR,
        sum(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE in ('Servers')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('Preferred', 'M2M Preferred'),
                MRR,
                0
            )
        ) as COMMAND_PREFERRED_SERVER_MRR,
        -----------------------------------------------------------------
        -- Other units
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Help Desk'),
                UNITS,
                0
            )
        ) AS HELP_DESK_UNITS,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                UNITS,
                0
            )
        ) AS SECURITY_UNITS,
        -----------------------------------------------------------------
        -- ARR
        -----------------------------------------------------------------
        (SUM(BILLINGS)) AS CURRENT_BILLINGS,
        (SUM(ARR)) AS CURRENT_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell',
                ARR,
                0
            )
        ) AS SELL_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge',
                ARR,
                0
            )
        ) AS BRIGHTGAUGE_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost',
                ARR,
                0
            )
        ) AS ITBOOST_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command') -- A.H.: update ?
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops', 'Networks', 'Servers'),
                ARR,
                0
            )
        ) AS COMMAND_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Third Party Patching'),
                ARR,
                0
            )
        ) AS RMM_ADDITIONAL_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk',
                ARR,
                0
            )
        ) AS HELP_DESK_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                AND lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) <> 'webroot',
                ARR,
                0
            )
        ) AS SECURITY_ARR,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                    AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                ),
                ARR,
                0
            )
        ) AS OTHER_ARR,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                1,
                0
            )
        ) AS SELL_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                1,
                0
            )
        ) AS BRIGHTGAUGE_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                1,
                0
            )
        ) AS ITBOOST_CLOUD,
        --            MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
        --                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
        --                        AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1,
        --                    0))                                                                   AS RMM_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk',
                1,
                0
            )
        ) AS HELP_DESK_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                1,
                0
            )
        ) AS SECURITY_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                    AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                )
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                1,
                0
            )
        ) AS OTHER_CLOUD,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                ARR,
                0
            )
        ) AS SELL_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                ARR,
                0
            )
        ) AS BRIGHTGAUGE_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                ARR,
                0
            )
        ) AS ITBOOST_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk',
                1,
                0
            )
        ) AS HELP_DESK_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                1,
                0
            )
        ) AS SECURITY_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                    AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                )
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                1,
                0
            )
        ) AS OTHER_LEGACY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                ARR,
                0
            )
        ) AS SELL_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                ARR,
                0
            )
        ) AS BRIGHTGAUGE_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                ARR,
                0
            )
        ) AS ITBOOST_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk',
                1,
                0
            )
        ) AS HELP_DESK_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                1,
                0
            )
        ) AS SECURITY_ON_PREM,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command')
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                )
                AND NOT (
                    PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security'
                    AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                )
                AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                1,
                0
            )
        ) AS OTHER_ON_PREM,
        SUM(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE LIKE '%Solution Partners%'
                AND lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) <> 'webroot',
                MRR,
                0
            )
        ) AS THIRD_PARTY_MRR,
        SUM(
            IFF(
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID <> '3P-SAAS3002315EPPRMM',
                MRR,
                0
            )
        ) AS WEBROOT_MRR,
        SUM(
            IFF(
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID <> '3P-SAAS3002315EPPRMM',
                UNITS,
                0
            )
        ) AS WEBROOT_UNITS,
        SUM(
            IFF(
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID = '3P-SAAS3002315EPPRMM',
                MRR,
                0
            )
        ) AS WEBROOT_OVERAGE_MRR,
        SUM(
            IFF(
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID = '3P-SAAS3002315EPPRMM',
                UNITS,
                0
            )
        ) AS WEBROOT_OVERAGE_UNITS
    FROM
        growth_obt
    where
        Company_name <> ''
    GROUP BY
        1,
        2,
        3
    HAVING
        SUM(BILLINGS) > 0
),
contract as (
    with fl as (
        select
            COMPANY_ID,
            COMPANY_NAME_WITH_ID,
            CONTRACT_NUMBER,
            PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT,
            min(START_DATE) as START_DATE,
            max(END_DATE) as END_DATE
        from
            ANALYTICS.DBO.GROWTH__OBT
        where
            METRIC_OBJECT = 'renewals'
        group by
            1,
            2,
            3,
            4
    ),
    sl as (
        select
            *,
            IFF(
                END_DATE < '2022-01-01' :: date,
                '2099-01-01' :: date,
                END_DATE
            ) as dayfilter,
            min(dayfilter) OVER (
                PARTITION BY COMPANY_ID
                order by
                    COMPANY_ID
            ) AS nearestdate,
            IFF(nearestdate = END_DATE, 1, 0) as daysfilterflag
        from
            fl
    )
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        END_DATE as Earliest_Date,
        listagg(CONTRACT_NUMBER, ',') as CONTRACT_NUMBER,
        listagg(PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT, ',') as Products
    from
        sl
    where
        daysfilterflag = 1
    group by
        1,
        2,
        3
),
customer_healthscores AS (
    SELECT
        SHIP_TO AS COMPANY_ID,
        HEALTHSCORE,
        HEALTHSCORE_ALPHA
    FROM
        ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__HEALTHSCORES QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SHIP_TO
            ORDER BY
                APDATE DESC
        ) = 1
),
customer_touch_tier AS (
    SELECT
        APPLIED_DATE,
        SHIP_TO,
        COUNT(DISTINCT TOUCH_TIER) AS TT_COUNT,
        LISTAGG(DISTINCT TOUCH_TIER, ' | ') WITHIN GROUP (
            ORDER BY
                TOUCH_TIER
        ) AS TT_CLASSES,
        MIN(TOUCH_TIER) AS TT_MIN,
        MAX(TOUCH_TIER) AS TT_MAX,
        MIN(TOUCH_TIER) AS TOUCH_TIER
    FROM
        ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__TECH_TOUCH_ROSTER
    GROUP BY
        1,
        2
),
customer_contract_type AS (
    SELECT
        APPLIED_DATE,
        SHIP_TO,
        LISTAGG(
            DISTINCT COALESCE(QUOTE_LINE_SUBSCRIPTION_TYPE, 'Non-Bedrock M2M'),
            ' | '
        ) WITHIN GROUP (
            ORDER BY
                COALESCE(QUOTE_LINE_SUBSCRIPTION_TYPE, 'Non-Bedrock M2M')
        ) AS CONTRACT_TYPE
    FROM
        ANALYTICS.dbo.CORE__RPT_BILLINGS
    GROUP BY
        1,
        2
),
-- A.H. : Do we need this or can we just set it from SKU packahe / plan  ?
customer_psa_package AS (
    SELECT
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        LISTAGG(
            DISTINCT IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('Basic', 'Standard', 'Premium'),
                PRODUCT_CATEGORIZATION_PRODUCT_PLAN,
                'Legacy'
            ),
            ' | '
        ) WITHIN GROUP (
            ORDER BY
                IFF(
                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage'
                    AND PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('Basic', 'Standard', 'Premium'),
                    PRODUCT_CATEGORIZATION_PRODUCT_PLAN,
                    'Legacy'
                )
        ) AS PSA_PACKAGE
    FROM
        growth_obt
    WHERE
        PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
        AND BILLINGS > 0
    GROUP BY
        1,
        2
),
current_monthly as (
    select
        arr.COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        product,
        "Seat Type",
        sum(BILLINGSLOCALUNIFIED) as "Current Monthly Total",
        case
            when product in ('Manage', 'Sell', 'BrightGauge', 'ItBoost')
            and (
                "Seat Type" = 'Include in ARR calculation'
                or "Seat Type" is null
            ) then sum(BILLINGSLOCALUNIFIED)
        end as cmp
    from
        arr_billings arr
--         left join (
--             select
--                 COMPANY_ID,
--                 COMPANY_NAME_ID
--             from
--                 DATAIKU.DEV_DATAIKU_STAGING.PNP_COMPANY_DIM
--         ) c on c.COMPANY_ID = ARR.COMPANY_ID
--     where
--         MSB_FLAG = 1
    group by
        1,
        2,
        3,
        4
),
monthly_price_cmp as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        sum("Current Monthly Total") as "Current Monthly Total",
        sum(cmp) as cmp
    from
        current_monthly
    group by
        1,
        2
),
current_monthly_rmm as (
    select
        arr.COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        product,
        "Seat Type",
        sum(BILLINGSLOCALUNIFIED) as "Current Monthly Total RMM",
        case
            when product in ('Automate', 'Command', 'CW RMM')
            and (
                "Seat Type" = 'Include in ARR calculation'
                or "Seat Type" is null
            ) then sum(BILLINGSLOCALUNIFIED)
        end as cmp_rmm
    from
        arr_billings arr
--         left join (
--             select
--                 COMPANY_ID,
--                 COMPANY_NAME_ID
--             from
--                 DATAIKU.DEV_DATAIKU_STAGING.PNP_COMPANY_DIM
--         ) c on c.COMPANY_ID = ARR.COMPANY_ID
--     where
--         MSB_FLAG = 1
    group by
        1,
        2,
        3,
        4
),
monthly_price_cmp_rmm as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        sum("Current Monthly Total RMM") as "Current Monthly Total RMM",
        sum(cmp_rmm) as cmp_rmm
    from
        current_monthly_rmm
    group by
        1,
        2
),
customer_tenure AS (
    SELECT
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        MIN(CORPORATE_BILLING_START_DATE) AS CORPORATE_START_DATE,
        MAX(CORPORATE_BILLING_REPORTING_PERIOD) AS CORPORATE_TENURE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Client & Process Mgmt',
                PRODUCT_GROUP_BILLING_START_DATE,
                NULL
            )
        ) AS PSA_START_DATE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Client & Process Mgmt',
                PRODUCT_GROUP_BILLING_REPORTING_PERIOD,
                NULL
            )
        ) AS PSA_TENURE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Remote Monitoring & Mgmt',
                PRODUCT_GROUP_BILLING_START_DATE,
                NULL
            )
        ) AS RMM_START_DATE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Remote Monitoring & Mgmt',
                PRODUCT_GROUP_BILLING_REPORTING_PERIOD,
                NULL
            )
        ) AS RMM_TENURE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_PORTFOLIO = 'Security Mgmt',
                PRODUCT_PORTFOLIO_BILLING_START_DATE,
                NULL
            )
        ) AS SECURITY_START_DATE,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_PORTFOLIO = 'Security Mgmt',
                PRODUCT_PORTFOLIO_BILLING_REPORTING_PERIOD,
                NULL
            )
        ) AS SECURITY_TENURE
    FROM
        analytics.DBO.growth__obt
    GROUP BY
        1,
        2
) -----------------------------------------------------------------
-----------------------------------------------------------------
-- Final table
-----------------------------------------------------------------
-----------------------------------------------------------------
SELECT
    distinct --removed duplicates
    -----------------------------------------------------------------
    -- From customer_roster
    -- Customer ID
    cr.COMPANY_ID,
    cr.COMPANY_NAME,
    cr.COMPANY_NAME_WITH_ID,
    CWS_ACCOUNT_UNIQUE_IDENTIFIER_C,
    -- Current ARR
    cr.CURRENT_ARR,
    cr.itnation_peer_group_active_partner,
    cr.manage_active_partner,
    cr.control_active_partner,
    cr.automate_active_partner,
    cr.sell_active_partner,
    cr.fortify_active_partner,
    cr.command_active_partner,
    cr.brightgauge_active_partner,
    cr.recover_active_partner,
    cr.help_desk_active_partner,
    cr.security_active_partner,
    cr.itboost_active_partner,
    cr.webroot_active_partner,
    cr.Automate_Units,
    cr.COMMAND_DESKTOP_UNITS,
    cr.COMMAND_NETWORK_UNITS,
    cr.COMMAND_SERVER_UNITS,
    cr.HELP_DESK_UNITS,
    cr.SECURITY_UNITS,
    (cr.SELL_ARR) AS SELL_ARR,
    (cr.BRIGHTGAUGE_ARR) AS BRIGHTGAUGE_ARR,
    (cr.ITBOOST_ARR) AS ITBOOST_ARR,
    (cr.RMM_ADDITIONAL_ARR) AS RMM_ADDITIONAL_ARR,
    (cr.HELP_DESK_ARR) AS HELP_DESK_ARR,
    (cr.SECURITY_ARR) AS SECURITY_ARR,
    (cr.OTHER_ARR) AS OTHER_ARR,
    (cr.COMMAND_ARR) AS COMMAND_ARR,
    cr.SELL_CLOUD,
    cr.SELL_LEGACY_ON_PREM,
    cr.SELL_ON_PREM,
    cr.BRIGHTGAUGE_CLOUD,
    cr.BRIGHTGAUGE_LEGACY_ON_PREM,
    cr.BRIGHTGAUGE_ON_PREM,
    cr.ITBOOST_CLOUD,
    cr.ITBOOST_LEGACY_ON_PREM,
    cr.ITBOOST_ON_PREM,
    cr.HELP_DESK_CLOUD,
    cr.HELP_DESK_LEGACY_ON_PREM,
    cr.HELP_DESK_ON_PREM,
    cr.SECURITY_CLOUD,
    cr.SECURITY_LEGACY_ON_PREM,
    cr.SECURITY_ON_PREM,
    cr.OTHER_CLOUD,
    cr.OTHER_LEGACY_ON_PREM,
    cr.OTHER_ON_PREM,
    cr.THIRD_PARTY_MRR,
    cr.WEBROOT_MRR,
    cr.WEBROOT_UNITS,
    cr.WEBROOT_OVERAGE_MRR,
    cr.WEBROOT_OVERAGE_UNITS,
    -----------------------------------------------------------------
    -- From customer_healthscores
    cast(chs.HEALTHSCORE as string) as HEALTHSCORE,
    --converted healthscore to string
    chs.HEALTHSCORE_ALPHA,
    iff(HEALTHSCORE_ALPHA = '0', 1, 0) as "Gainsight Score Available",
    concat(HEALTHSCORE_ALPHA, '-', HEALTHSCORE) as "Health Score Grade",
    -----------------------------------------------------------------
    -- From customer_touch_tier
    COALESCE(
        ctt.TOUCH_TIER,
        'Tech Touch (due to non-qualifying MRR)'
    ) AS TOUCH_TIER,
    case
        when TOUCH_TIER <> '0' then TOUCH_TIER
        when TOUCH_TIER = '0' then 'None'
        else null
    end as "Deal Value",
    -----------------------------------------------------------------
    -- From customer_contract_type
    cct.CONTRACT_TYPE,
    ------------------------------------------------------------------
    -- From customer tenure
    ct.CORPORATE_START_DATE,
    ct.CORPORATE_TENURE,
    ct.PSA_START_DATE,
    ct.PSA_TENURE,
    ct.RMM_START_DATE,
    ct.RMM_TENURE,
    ct.SECURITY_START_DATE,
    ct.SECURITY_TENURE,
    -----------------------------------------------------------------
    -- From customer_psa_package
    cpp.PSA_PACKAGE,
    -----------------------------------------------------------------
    -- From contract
    c.CONTRACT_NUMBER,
    c.Earliest_Date,
    c.Products,
    -----------------------------------------------------------------
    -- from automate_and_manage_cal table (amc)
    -- start of expanded columns and removed amc.company_id, manage category, and automate category
    iff(
        PSA_LEGACY_ON_PREM is null,
        0,
        PSA_LEGACY_ON_PREM
    ) as PSA_LEGACY_ON_PREM,
    iff(PSA_ON_PREM is null, 0, PSA_ON_PREM) as PSA_ON_PREM,
    iff(PSA_CLOUD is null, 0, PSA_CLOUD) as PSA_CLOUD,
    AUTOMATE_LEGACY_ON_PREM as RMM_LEGACY_ON_PREM,
    -- changed name to rmm legacy on prem
    AUTOMATE_LEGACY_ON_PREM,
    AUTOMATE_ON_PREM as RMM_ON_PREM,
    -- changed name to rmm on prem,
    AUTOMATE_ON_PREM,
    AUTOMATE_CLOUD as RMM_CLOUD,
    --Changed name to rmm cloud
    AUTOMATE_CLOUD,
    iff(PSA_ARR is null, 0, PSA_ARR) as PSA_ARR,
    AUTOMATE_ARR,
    iff(PSA_UNITS is null, 0, PSA_UNITS) as PSA_UNITS,
    -- AUTOMATE_UNITS, -- getting it difrectly from here now
    iff(PSA_ON_PREM_ARR is null, 0, PSA_ON_PREM_ARR) as PSA_ON_PREM_ARR,
    iff(PSA_CLOUD_ARR is null, 0, PSA_CLOUD_ARR) as PSA_CLOUD_ARR,
    iff(
        PSA_LEGACY_ON_PREM_ARR is null,
        0,
        PSA_LEGACY_ON_PREM_ARR
    ) as PSA_LEGACY_ON_PREM_ARR,
    cast(
        iff(PSA_ON_PREM_UNITS is null, 0, PSA_ON_PREM_UNITS) as int
    ) as PSA_ON_PREM_UNITS,
    iff(PSA_CLOUD_UNITS is null, 0, PSA_CLOUD_UNITS) as PSA_CLOUD_UNITS,
    cast(
        iff(
            PSA_LEGACY_ON_PREM_UNITS is null,
            0,
            PSA_LEGACY_ON_PREM_UNITS
        ) as int
    ) as PSA_LEGACY_ON_PREM_UNITS,
    iff(
        AUTOMATE_CLOUD_ARR is null,
        0,
        AUTOMATE_CLOUD_ARR
    ) as AUTOMATE_CLOUD_ARR,
    iff(
        AUTOMATE_ON_PREM_ARR is null,
        0,
        AUTOMATE_ON_PREM_ARR
    ) as AUTOMATE_ON_PREM_ARR,
    iff(
        AUTOMATE_LEGACY_ON_PREM_ARR is null,
        0,
        AUTOMATE_LEGACY_ON_PREM_ARR
    ) as AUTOMATE_LEGACY_ON_PREM_ARR,
    iff(
        AUTOMATE_CLOUD_UNITS is null,
        0,
        AUTOMATE_CLOUD_UNITS
    ) as AUTOMATE_CLOUD_UNITS,
    iff(
        AUTOMATE_ON_PREM_UNITS is null,
        0,
        AUTOMATE_ON_PREM_UNITS
    ) as AUTOMATE_ON_PREM_UNITS,
    cast(
        iff(
            AUTOMATE_LEGACY_ON_PREM_UNITS is null,
            0,
            AUTOMATE_LEGACY_ON_PREM_UNITS
        ) as int
    ) as AUTOMATE_LEGACY_ON_PREM_UNITS,
    -----------------------------------------------------------------
    -- hybrid flag
    case
        when PSA_ON_PREM = 1 then 1
        when SELL_ON_PREM = 1 then 1
        when BRIGHTGAUGE_ON_PREM = 1 then 1
        when ITBOOST_ON_PREM = 1 then 1
        when RMM_ON_PREM = 1 then 1
        when SECURITY_ON_PREM = 1 then 1
        when OTHER_ON_PREM = 1 then 1
        else 0
    end as hybrid_flag,
    -----------------------------------------------------------------
    --on prem flag
    case
        when PSA_LEGACY_ON_PREM = 1 then 1
        when SELL_LEGACY_ON_PREM = 1 then 1
        when BRIGHTGAUGE_LEGACY_ON_PREM = 1 then 1
        when ITBOOST_LEGACY_ON_PREM = 1 then 1
        when HELP_DESK_LEGACY_ON_PREM = 1 then 1
        when SECURITY_LEGACY_ON_PREM = 1 then 1
        when OTHER_LEGACY_ON_PREM = 1 then 1
        else 0
    end as "On-Prem Flag",
    (hybrid_flag + "On-Prem Flag") as "On-Prem/Hybrid",
    case
        when HEALTHSCORE_ALPHA = 'A' then 'A-B'
        when HEALTHSCORE_ALPHA = 'B' then 'A-B'
        when HEALTHSCORE_ALPHA = 'C' then 'C'
        when HEALTHSCORE_ALPHA = 'D' then 'D-F'
        when HEALTHSCORE_ALPHA = 'F' then 'F'
        else 'None'
    end as "Gainsight Risk",
    IFF(
        itnation_peer_group_active_partner = 1,
        'Active Member',
        'No'
    ) as "IT Nation",
    -------------------------------------------------------------
    -- RMM package assignment
    -------------------------------------------------------------
    (AUTOMATE_ARR + cr.COMMAND_ARR) as RMM_ARR,
    iff(
        automate_active_partner > 0,
        'Essentials WO RPP',
        iff(
            command_active_partner > 0,
            'Pro W EPP',
            'Undefined'
        )
    ) as future_RMM,
    ------------
    -- Old logic
    -- case
    --     when automate_active_partner > 0
    --         and webroot_active_partner > 0
    --         and brightgauge_active_partner = 0 then 'CW-RMM-EPB-STANDARD'
    --     when automate_active_partner > 0
    --         and webroot_active_partner > 0
    --         and brightgauge_active_partner > 0 then 'CW-RMM--ADVANCED-EPP'
    --     when automate_active_partner > 0
    --         and webroot_active_partner = 0
    --         and brightgauge_active_partner = 0 then 'CWRMMEPBSTND-W-O-EPP'
    --     when automate_active_partner > 0
    --         and webroot_active_partner = 0
    --         and brightgauge_active_partner > 0 then 'CW-RMM-ADV-WOUT-EPP'
    --     else 'None'
    --     end                                          as          "RMM Package",
    -----------------------------
    -- Place holder so PBI reloads
    'NA' as "RMM Package",
    0 as AUTOMATE_UNITS_CALC,
    iff(cr.AUTOMATE_UNITS > 0, 0, 0) as AUTOMATE_UNITS_CALC,
    -- Old logic :
    -- iff(
    --         command_active_partner = 1,
    --         (
    --                 COMMAND_DESKTOP_UNITS + COMMAND_SERVER_UNITS + HELP_DESK_UNITS
    --         ),
    --         0
    -- ) as RMM_Units_Additive,
    0 as RMM_Units_Additive,
    -- (
    --         COMMAND_DESKTOP_UNITS + COMMAND_NETWORK_UNITS + COMMAND_SERVER_UNITS + AUTOMATE_UNITS
    -- ) as RMM_UNITS,
    iff(
        automate_active_partner > 0,
        cr.AUTOMATE_UNITS,
        iff(
            command_active_partner > 0,
            COMMAND_TOTAL_UNITS,
            null
        )
    ) as RMM_UNITS,
    -----------------
    -- case
    --         when future_RMM = 'Pro W EPP' then cast(min(rmmpb."CW-RMM-ADV-WOUT-EPP") as float)
    --         when future_RMM = 'Essentials WO RPP' then cast(min(rmmpb."CW-RMM-EPB-STANDARD") as float)
    --         when future_RMM = 'Undefined' then cast(min(rmmpb."CW-RMM--ADVANCED-EPP") as float)
    -- end as Price_Per_Seat_RMM,
    case
        when automate_active_partner > 0 then cast(min(rmmpb."CW-RMM-EPB-STANDARD") as float)
        when command_active_partner > 0 then cast(min(rmmpb."CW-RMM--ADVANCED-EPP") as float)
        else null
    end as Price_Per_Seat_RMM,
    -----------------
    -- case
    --         when future_RMM = 'Pro W EPP' then cast(max(rmmpb."CW-RMM-ADV-WOUT-EPP") as float)
    --         when future_RMM = 'Essentials WO RPP' then cast(max(rmmpb."CW-RMM-EPB-STANDARD") as float)
    --         when future_RMM = 'Undefined' then cast(max(rmmpb."CW-RMM--ADVANCED-EPP") as float)
    -- end as List_Price_RMM,
    case
        when automate_active_partner > 0 then cast(max(rmmpb."CW-RMM-EPB-STANDARD") as float)
        when command_active_partner > 0 then cast(max(rmmpb."CW-RMM--ADVANCED-EPP") as float)
    end as List_Price_RMM,
    -----------------------
    (RMM_UNITS * Price_Per_Seat_RMM) as "Future Monthly Price RMM",
    "Current Monthly Total RMM",
    cmp_rmm,
    ("Future Monthly Price RMM" - cmp_rmm) / nullifzero(cmp_rmm) "Monthly Price Increase RMM %",
    -- A.H. : Needs to be updated :
    -- case
    --         when (
    --                 RMM_UNITS + RMM_Units_Additive + AUTOMATE_LEGACY_ON_PREM_UNITS
    --         ) = 0 then null
    --         when (
    --                 command_active_partner = 1
    --                 and automate_active_partner = 1
    --         ) then RMM_UNITS
    --         when (
    --                 command_active_partner = 1
    --                 and automate_active_partner = 0
    --         ) then RMM_Units_Additive
    --         when (
    --                 command_active_partner = 0
    --                 and automate_active_partner = 1
    --         ) then RMM_UNITS
    --         else 0
    -- end as "Total RMM Units",
    -------------------------------------------------------------
    -- PSA package assignment
    -------------------------------------------------------------
    -- PSA_PACKAGE, A.H. : Duplicate column?
    case
        when PSA_PACKAGE = 'Premium' then 'Best'
        when PSA_PACKAGE = 'Legacy | Premium' then 'Best'
        when PSA_PACKAGE = 'Premium | Standard' then 'Best'
        when PSA_PACKAGE = 'Basic | Legacy | Standard' then 'Best'
        when PSA_PACKAGE = 'Legacy' then 'Better'
        when PSA_PACKAGE = 'Standard' then 'Better'
        when PSA_PACKAGE = 'Legacy | Standard' then 'Better'
        when PSA_PACKAGE = 'Basic' then 'Good'
        when PSA_PACKAGE = 'Basic | Legacy' then 'Good'
        when PSA_PACKAGE = 'Basic | Standard' then 'Good'
        when PSA_PACKAGE is null then null
    end as Legacy,
    case
        when SELL_ACTIVE_PARTNER > 0 then 'Best'
        when BRIGHTGAUGE_ACTIVE_PARTNER = 0
        and Legacy is not null then Legacy
        when MANAGE_ACTIVE_PARTNER > 0
        and BRIGHTGAUGE_ACTIVE_PARTNER > 0 then 'Better'
        else 'None'
    end as "PSA Package Active Use FINAL",
    case
        when "PSA Package Active Use FINAL" = 'Better' then 'Bus Mgmt Standard'
        when "PSA Package Active Use FINAL" = 'Best' then 'Bus Mgmt Advanced'
        when "PSA Package Active Use FINAL" = 'Good' then 'Bus Mgmt Core'
        else null
    end as Future,
    case
        when Future = 'Bus Mgmt Advanced' then max(pb."Best")
        when Future = 'Bus Mgmt Standard' then max(pb."Better")
        when Future = 'Bus Mgmt Core' then max(pb."Good")
        else null
    end as "List Price",
    case
        when "PSA Package Active Use FINAL" = 'Better' then min(pb."Better")
        when "PSA Package Active Use FINAL" = 'Best' then min(pb."Best")
        when "PSA Package Active Use FINAL" = 'Good' then min(pb."Good")
        ELSE 0
    end as "Bus Mgmt Future Price Per Seat",
    (PSA_UNITS * "Bus Mgmt Future Price Per Seat") as "Future Monthly Price",
    "Current Monthly Total",
    cmp,
    ("Future Monthly Price" - cmp) / nullifzero(cmp) "Monthly Price Increase %",
    --------------------------------------------------------
    --Pricebook details (A.H. needs checking)
    max(pb.LOWERBOUND) as max_lowerbound,
    max(rmmpb.lowerbound) as max_lowerbound_rmm,
    max(REFERENCE_CURRENCY) as REFERENCE_CURRENCY
FROM
    customer_roster cr
    LEFT JOIN contract c ON c.COMPANY_NAME_WITH_ID = cr.COMPANY_NAME_WITH_ID
    LEFT JOIN customer_healthscores chs ON chs.COMPANY_ID = cr.COMPANY_ID
    LEFT JOIN customer_touch_tier ctt ON ctt.APPLIED_DATE = cr.REPORTING_DATE
    AND ctt.SHIP_TO = cr.COMPANY_ID
    LEFT JOIN customer_contract_type cct ON cct.APPLIED_DATE = cr.REPORTING_DATE
    AND cct.SHIP_TO = cr.COMPANY_ID
    LEFT JOIN customer_psa_package cpp ON cpp.COMPANY_ID = cr.COMPANY_ID
    LEFT JOIN customer_tenure ct ON ct.COMPANY_NAME_WITH_ID = cr.COMPANY_NAME_WITH_ID
    LEFT JOIN automate_manage_calc amc on amc.COMPANY_NAME_WITH_ID = cr.COMPANY_NAME_WITH_ID --merged queries
    left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_ARR_AND_BILLING_C arr_c on arr_c.COMPANY_NAME_WITH_ID = cr.COMPANY_NAME_WITH_ID
    left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_BUSINESS_MANAGEMENT_PRICEBOOK_STAGING pb on pb.CUR = REFERENCE_CURRENCY
    and pb.LOWERBOUND <= PSA_UNITS
    left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_RMM_PRICEBOOK_STAGING rmmpb on rmmpb.CUR = REFERENCE_CURRENCY
    and rmmpb.LOWERBOUND <= (
        --COMMAND_SERVER_UNITS + COMMAND_NETWORK_UNITS + COMMAND_DESKTOP_UNITS + cr.AUTOMATE_UNITS +
        iff(
            automate_active_partner > 0,
            cr.AUTOMATE_UNITS,
            iff(
                command_active_partner > 0,
                COMMAND_TOTAL_UNITS,
                0
            )
        )
    )
    left join monthly_price_cmp on cr.COMPANY_NAME_WITH_ID = monthly_price_cmp.COMPANY_NAME_WITH_ID
    left join monthly_price_cmp_rmm on cr.COMPANY_NAME_WITH_ID = monthly_price_cmp_rmm.COMPANY_NAME_WITH_ID
    left join (
        select
            distinct id,
            CWS_ACCOUNT_UNIQUE_IDENTIFIER_C
        from
            ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__ACCOUNT
    ) bsa on bsa.ID = cr.COMPANY_ID
where
    CURRENT_ARR <> 0 --filtered current arr to not be 0
    and cr.COMPANY_ID not in (
        'lopez@cinformatique.ch',
        'JEREMY.A.BECKER@GMAIL.COM',
        'blairphillips@gmail.com',
        'Chad@4bowers.net',
        'dev@bcsint.com',
        'bob@compu-gen.com',
        'Greg@ablenetworksnj.com',
        'screenconnect.com@solutionssquad.com',
        'andrew@gmal.co.uk',
        '144274'
    ) -- filtered rows to exclude these
group by
    cr.COMPANY_ID,
    cr.COMPANY_NAME_WITH_ID,
    CWS_ACCOUNT_UNIQUE_IDENTIFIER_C,
    cr.COMPANY_NAME,
    cr.CURRENT_ARR,
    cr.itnation_peer_group_active_partner,
    cr.manage_active_partner,
    cr.control_active_partner,
    cr.automate_active_partner,
    cr.sell_active_partner,
    cr.fortify_active_partner,
    cr.command_active_partner,
    cr.brightgauge_active_partner,
    cr.recover_active_partner,
    cr.help_desk_active_partner,
    cr.security_active_partner,
    cr.itboost_active_partner,
    cr.webroot_active_partner,
    cr.Automate_Units,
    cr.COMMAND_DESKTOP_UNITS,
    cr.COMMAND_NETWORK_UNITS,
    cr.COMMAND_SERVER_UNITS,
    cr.HELP_DESK_UNITS,
    cr.SECURITY_UNITS,
    SELL_ARR,
    BRIGHTGAUGE_ARR,
    ITBOOST_ARR,
    RMM_ADDITIONAL_ARR,
    HELP_DESK_ARR,
    SECURITY_ARR,
    OTHER_ARR,
    COMMAND_ARR,
    cr.SELL_CLOUD,
    cr.SELL_LEGACY_ON_PREM,
    cr.SELL_ON_PREM,
    cr.BRIGHTGAUGE_CLOUD,
    cr.BRIGHTGAUGE_LEGACY_ON_PREM,
    cr.BRIGHTGAUGE_ON_PREM,
    cr.ITBOOST_CLOUD,
    cr.ITBOOST_LEGACY_ON_PREM,
    cr.ITBOOST_ON_PREM,
    cr.HELP_DESK_CLOUD,
    cr.HELP_DESK_LEGACY_ON_PREM,
    cr.HELP_DESK_ON_PREM,
    cr.SECURITY_CLOUD,
    cr.SECURITY_LEGACY_ON_PREM,
    cr.SECURITY_ON_PREM,
    cr.OTHER_CLOUD,
    cr.OTHER_LEGACY_ON_PREM,
    cr.OTHER_ON_PREM,
    cr.THIRD_PARTY_MRR,
    cr.WEBROOT_MRR,
    cr.WEBROOT_UNITS,
    cr.WEBROOT_OVERAGE_MRR,
    cr.WEBROOT_OVERAGE_UNITS,
    HEALTHSCORE,
    HEALTHSCORE_ALPHA,
    TOUCH_TIER,
    "Deal Value",
    cct.CONTRACT_TYPE,
    ct.CORPORATE_START_DATE,
    ct.CORPORATE_TENURE,
    ct.PSA_START_DATE,
    ct.PSA_TENURE,
    ct.RMM_START_DATE,
    ct.RMM_TENURE,
    ct.SECURITY_START_DATE,
    ct.SECURITY_TENURE,
    cpp.PSA_PACKAGE,
    c.CONTRACT_NUMBER,
    c.Earliest_Date,
    c.Products,
    PSA_LEGACY_ON_PREM,
    PSA_ON_PREM,
    PSA_CLOUD,
    AUTOMATE_LEGACY_ON_PREM,
    AUTOMATE_ON_PREM,
    AUTOMATE_CLOUD,
    PSA_ARR,
    PSA_UNITS,
    PSA_ON_PREM_ARR,
    PSA_CLOUD_ARR,
    PSA_LEGACY_ON_PREM_ARR,
    PSA_ON_PREM_UNITS,
    PSA_CLOUD_UNITS,
    PSA_LEGACY_ON_PREM_UNITS,
    AUTOMATE_CLOUD_ARR,
    AUTOMATE_ON_PREM_ARR,
    AUTOMATE_LEGACY_ON_PREM_ARR,
    AUTOMATE_CLOUD_UNITS,
    AUTOMATE_ON_PREM_UNITS,
    AUTOMATE_LEGACY_ON_PREM_UNITS,
    AUTOMATE_ARR,
    CR.COMMAND_TOTAL_UNITS,
    "Current Monthly Total RMM",
    cmp_rmm,
    "Current Monthly Total",
    cmp
