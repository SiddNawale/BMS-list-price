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
--                      -- DATAIKU.ENV.PNP_DASHBOARD_ARR_AND_BILLING
--                              --- current_monthly CTE, current_monthly_rmm CTE
--                      --  DATAIKU.ENV.PNP_DASHBOARD_BUSINESS_MANAGEMENT_PRICEBOOK_STAGING
--                              --- Pricebook details for BMS (A.H. should unify it with RMM)
--                      -- DATAIKU.ENV.PNP_DASHBOARD_RMM_PRICEBOOK_STAGING
--                              -- Pricebook details for RMM
--                      --DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
--                              -- Reporting date limit
-- Output       - DATAIKU.ENV.pnp_dashboard_migration_query
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
--                      --has risk score, risk level, and risk to migrate logic
--                      -- bms and rmm future monthly prices and monthly price increases
--                      -- BMS Package and RMM flags are showing if they are already billed for BMS/CW RMM
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
--              - As of May, 2023
--                      --merged intermediate queries (e.g. Manage_automate_calc query)
--                      --'🙈 nothing to see here' has been replaced by "NA-dbt_value"
--                      -- SKP Risk Score Logic has been added
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
        DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_ARR_AND_BILLING
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
        UNITS_INCLUDE_FLAG --                                       MSB_FLAG
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
        --                 MSB_FLAG,
        SUM(
            iff(
                "Brand" = 'Manage'
                and "Seat Type" = 'Include in BMS ARR calculation',
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
customer_roster_intermediate as (
    SELECT
        REPORTING_DATE,
        COMPANY_NAME_WITH_ID,
        PRODUCT_CATEGORIZATION_PRODUCT_LINE,
        rtrim(ltrim(COMPANY_ID)) as COMPANY_ID,
        --trimmed companyy i
        min(
            iff(
                COMPANY_NAME = '🙈 nothing to see here',
                null,
                COMPANY_NAME
            )
        ) as COMPANY_NAME,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Business Mgmt Packages',
                1,
                0
            )
        ) AS has_BMS_package,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT in ('Manage', 'Sell', 'BrightGauge'),
                1,
                0
            )
        ) AS MSB_FLAG,
        MAX(
            IFF(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'CW RMM',
                1,
                0
            )
        ) AS has_CW_RMM,
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
        3,
        4
    HAVING
        SUM(BILLINGS) > 0
),
customer_roster as (
    select
        reporting_date,
        company_name_with_id,
        company_name,
        company_id,
        max(has_bms_package) as has_bms_package,
        max(msb_flag) as msb_flag,
        max(has_cw_rmm) as has_cw_rmm,
        max(sell_active_partner) as sell_active_partner,
        max(fortify_active_partner) as fortify_active_partner,
        max(manage_active_partner) as manage_active_partner,
        max(control_active_partner) as control_active_partner,
        max(automate_active_partner) as automate_active_partner,
        max(command_active_partner) as command_active_partner,
        max(brightgauge_active_partner) as brightgauge_active_partner,
        max(recover_active_partner) as recover_active_partner,
        max(help_desk_active_partner) as help_desk_active_partner,
        max(itboost_active_partner) as itboost_active_partner,
        max(security_active_partner) as security_active_partner,
        max(itnation_active_partner) as itnation_active_partner,
        max(itnation_peer_group_active_partner) as itnation_peer_group_active_partner,
        max(webroot_active_partner) as webroot_active_partner,
        max(automate_units) as automate_units,
        sum(command_total_units) as command_total_units,
        sum(command_desktop_units) as command_desktop_units,
        sum(command_network_units) as command_network_units,
        sum(command_server_units) as command_server_units,
        sum(command_essential_desktop_units) as command_essential_desktop_units,
        sum(command_essential_server_units) as command_essential_server_units,
        sum(command_elite_server_units) as command_elite_server_units,
        sum(command_preferred_server_units) as command_preferred_server_units,
        sum(command_helpdesk_units) as command_helpdesk_units,
        sum(command_mrr) as command_mrr,
        sum(command_hd_mrr) as command_hd_mrr,
        sum(command_desktop_mrr) as command_desktop_mrr,
        sum(command_server_mrr) as command_server_mrr,
        sum(command_essential_desktop_mrr) as command_essential_desktop_mrr,
        sum(command_essential_server_mrr) as command_essential_server_mrr,
        sum(command_elite_server_mrr) as command_elite_server_mrr,
        sum(command_preferred_server_mrr) as command_preferred_server_mrr,
        sum(help_desk_units) as help_desk_units,
        sum(security_units) as security_units,
        sum(current_billings) as current_billings,
        sum(current_arr) as current_arr,
        sum(sell_arr) as sell_arr,
        sum(brightgauge_arr) as brightgauge_arr,
        sum(itboost_arr) as itboost_arr,
        sum(command_arr) as command_arr,
        sum(rmm_additional_arr) as rmm_additional_arr,
        sum(help_desk_arr) as help_desk_arr,
        sum(security_arr) as security_arr,
        sum(other_arr) as other_arr,
        max(sell_cloud) as sell_cloud,
        max(brightgauge_cloud) as brightgauge_cloud,
        max(itboost_cloud) as itboost_cloud,
        max(help_desk_cloud) as help_desk_cloud,
        max(security_cloud) as security_cloud,
        max(other_cloud) as other_cloud,
        max(sell_legacy_on_prem) as sell_legacy_on_prem,
        max(brightgauge_legacy_on_prem) as brightgauge_legacy_on_prem,
        max(itboost_legacy_on_prem) as itboost_legacy_on_prem,
        max(help_desk_legacy_on_prem) as help_desk_legacy_on_prem,
        max(security_legacy_on_prem) as security_legacy_on_prem,
        max(other_legacy_on_prem) as other_legacy_on_prem,
        max(sell_on_prem) as sell_on_prem,
        max(brightgauge_on_prem) as brightgauge_on_prem,
        max(itboost_on_prem) as itboost_on_prem,
        max(help_desk_on_prem) as help_desk_on_prem,
        max(security_on_prem) as security_on_prem,
        max(other_on_prem) as other_on_prem,
        sum(third_party_mrr) as third_party_mrr,
        sum(webroot_mrr) as webroot_mrr,
        sum(webroot_units) as webroot_units,
        sum(webroot_overage_mrr) as webroot_overage_mrr,
        sum(webroot_overage_units) as webroot_overage_units
    from
        customer_roster_intermediate
    group by
        1,
        2,
        3,
        4
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
                "Seat Type" = 'Include in BMS ARR calculation'
                or "Seat Type" is null
            ) then sum(BILLINGSLOCALUNIFIED)
        end as cmp,
        case
            when product in ('Manage', 'Sell', 'BrightGauge', 'ItBoost')
            and (
                "Seat Type" = 'Exclude from ARR Calculation'
            ) then sum(BILLINGSLOCALUNIFIED)
        end as excluded_BMS
    from
        arr_billings arr --                  left join (
        --                  select
        --                      COMPANY_ID,
        --                      COMPANY_NAME_ID
        --                  from
        --                      DATAIKU.DEV_DATAIKU_STAGING.PNP_COMPANY_DIM
        --              ) c on c.COMPANY_ID = ARR.COMPANY_ID
    where
        MSB_FLAG = 1
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
        sum(cmp) as cmp,
        sum(excluded_BMS) as excluded_BMS
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
            when product in ('Automate', 'Command', 'Help Desk')
            and (
                "Seat Type" = 'Include in RMM ARR calculation'
                or "Seat Type" is null
            ) then sum(BILLINGSLOCALUNIFIED)
        end as cmp_rmm,
        sum(HD_total_monthly_future_price_by_sku) as HD_total_monthly_future_price_by_sku,
        sum(noc_future_monthly_price) as noc_future_monthly_price
    from
        arr_billings arr --         left join (
        --             select
        --                 COMPANY_ID,
        --                 COMPANY_NAME_ID
        --             from
        --                 DATAIKU.DEV_DATAIKU_STAGING.PNP_COMPANY_DIM
        --         ) c on c.COMPANY_ID = ARR.COMPANY_ID
    where
        product in ('Automate', 'Command', 'Help Desk')
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
        sum(cmp_rmm) as cmp_rmm,
        sum(HD_total_monthly_future_price_by_sku) as HD_total_monthly_future_price_by_sku,
        sum(noc_future_monthly_price) as noc_future_monthly_price
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
),
-----------------------------------------------------------------
-----------------------------------------------------------------
-- Final table
-----------------------------------------------------------------
-----------------------------------------------------------------
final_table as (
    SELECT
        distinct --removed duplicates
        -----------------------------------------------------------------
        -- From customer_roster
        -- Customer ID
        cr.COMPANY_ID,
        cr.COMPANY_NAME,
        cr.COMPANY_NAME_WITH_ID,
        CWS_ACCOUNT_UNIQUE_IDENTIFIER_C,
        cr.reporting_date,
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
            'CW RMM Essentials',
            iff(
                command_active_partner > 0,
                'CW RMM Pro',
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
        --         when future_RMM = 'CW RMM Pro' then cast(min(rmmpb."CW-RMM-ADV-WOUT-EPP") as float)
        --         when future_RMM = 'Essentials WO RPP' then cast(min(rmmpb."CW-RMM-EPB-STANDARD") as float)
        --         when future_RMM = 'Undefined' then cast(min(rmmpb."CW-RMM--ADVANCED-EPP") as float)
        -- end as Price_Per_Seat_RMM,
        case
            when automate_active_partner > 0 then cast(min(rmmpb."ConnectWise RMM Essentials") as float)
            when command_active_partner > 0 then cast(min(rmmpb."ConnectWise RMM Pro") as float)
            else null
        end as Price_Per_Seat_RMM,
        -----------------
        -- case
        --         when future_RMM = 'CW RMM Pro' then cast(max(rmmpb."CW-RMM-ADV-WOUT-EPP") as float)
        --         when future_RMM = 'Essentials WO RPP' then cast(max(rmmpb."CW-RMM-EPB-STANDARD") as float)
        --         when future_RMM = 'Undefined' then cast(max(rmmpb."CW-RMM--ADVANCED-EPP") as float)
        -- end as List_Price_RMM,
        case
            when automate_active_partner > 0 then cast(max(rmmpb."ConnectWise RMM Essentials") as float)
            when command_active_partner > 0 then cast(max(rmmpb."ConnectWise RMM Pro") as float)
        end as List_Price_RMM,
        -----------------------
        (RMM_UNITS * Price_Per_Seat_RMM) as "Future Monthly Price RMM",
        "Current Monthly Total RMM",
        cmp_rmm,
        monthly_price_cmp_rmm.noc_future_monthly_price,
        monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku,
        ("Future Monthly Price RMM" - cmp_rmm) / nullifzero(cmp_rmm) "Monthly Software Price Increase RMM %",

        --         monthly_price_cmp_rmm.noc_future_monthly_price + monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku + "Future Monthly Price RMM" as future_monthly_total_RMM,
        iff(
            monthly_price_cmp_rmm.noc_future_monthly_price is null,
            0,
            monthly_price_cmp_rmm.noc_future_monthly_price
        ) + iff(
            monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku is null,
            0,
            monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku
        ) + iff(
            "Future Monthly Price RMM" is null,
            0,
            "Future Monthly Price RMM"
        ) as future_monthly_total_RMM,
        (
            --             monthly_price_cmp_rmm.noc_future_monthly_price + monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku + "Future Monthly Price RMM" - "Current Monthly Total RMM"
            iff(
                monthly_price_cmp_rmm.noc_future_monthly_price is null,
                0,
                monthly_price_cmp_rmm.noc_future_monthly_price
            ) + iff(
                monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku is null,
                0,
                monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku
            ) + iff(
                "Future Monthly Price RMM" is null,
                0,
                "Future Monthly Price RMM"
              
            )
              - "Current Monthly Total RMM"
        ) /nullifzero("Current Monthly Total RMM") as "Total Monthly Price Increase RMM %",
            
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
            when "PSA Package Active Use FINAL" = 'Better' then 'Bus Mgmt Pro'
            when "PSA Package Active Use FINAL" = 'Best' then 'Bus Mgmt Premium'
            when "PSA Package Active Use FINAL" = 'Good' then 'Bus Mgmt Essentials'
            else null
        end as Future,
        --                     MSB_FLAG,
        case
            when Future = 'Bus Mgmt Premium' then max(pb."Best")
            when Future = 'Bus Mgmt Pro' then max(pb."Better")
            when Future = 'Bus Mgmt Essentials' then max(pb."Good")
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
        excluded_BMS,
        ("Future Monthly Price" - cmp) / nullifzero(cmp) "Monthly Price Increase %",
        (
            "Future Monthly Price" + iff(excluded_BMS is null, 0, excluded_BMS)
        ) as future_monthly_total_BMS,
        --------------------------------------------------------
        --Pricebook details (A.H. needs checking)
        max(pb.LOWERBOUND) as max_lowerbound,
        max(rmmpb.lowerbound) as max_lowerbound_rmm,
        max(REFERENCE_CURRENCY) as REFERENCE_CURRENCY,
        has_BMS_package,
        has_CW_RMM,
        MSB_FLAG,
        date_trunc('day', cast(GETDATE() as date)) AS RUN_DATE
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
        left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_ARR_AND_BILLING arr_c on arr_c.COMPANY_NAME_WITH_ID = cr.COMPANY_NAME_WITH_ID
        left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_BUSINESS_MANAGEMENT_PRICEBOOK pb on pb.CUR = REFERENCE_CURRENCY
        and pb.LOWERBOUND <= PSA_UNITS
        left join DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_RMM_PRICEBOOK rmmpb on rmmpb.CUR = REFERENCE_CURRENCY
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
        -- and cr.COMPANY_NAME_WITH_ID = 'Visual Edge Inc. (0016g00000pU2CnAAK)'
    group by
        cr.COMPANY_ID,
        cr.COMPANY_NAME_WITH_ID,
        CWS_ACCOUNT_UNIQUE_IDENTIFIER_C,
        cr.reporting_date,
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
        cmp,
        has_BMS_package,
        has_CW_RMM,
        MSB_FLAG,
        monthly_price_cmp_rmm.noc_future_monthly_price,
        monthly_price_cmp_rmm.HD_total_monthly_future_price_by_sku,
        excluded_BMS
),
"Current ARR All Categories" as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        sum(ARR_UNIFIED) as ARR_UNIFIED_sum
    from
        arr_billings arr
    where
        REPORTING_DATE = (
            select
                max(REPORTING_DATE)
            from
                dataiku.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
    group by
        1,
        2
),
last_year_arr as (
    select
        COMPANY_ID,
        COMPANY_NAME_WITH_ID,
        REPORTING_DATE,
        sum(ARR_UNIFIED) as last_year_arr
    from
        DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_ARR_AND_BILLING
    where
        REPORTING_DATE = (
            select
                DATEADD(year, -1, max(REPORTING_DATE))
            from
                dataiku.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
    group by
        1,
        2,
        3
)
select
    f.*,
    ---------------------------------------------------------
    ------- final score calculation ------
    ---------------------------------------------------------
    case
        when CORPORATE_TENURE >= 72 then 3
        when CORPORATE_TENURE <= 12 then 0
        when CORPORATE_TENURE >= 60 then 2
        else 1
    end as customer_tenure_points,
    case
        when CONTRACT_TYPE = 'Non-Bedrock M2M | Renewable' then 'Contracted'
        when CONTRACT_TYPE = 'Renewable' then 'Contracted'
        when CONTRACT_TYPE = 'Evergreen | Non-Bedrock M2M | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'Non-Bedrock M2M | One-time | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'One-time | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen | Non-Bedrock M2M | One-time | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen | One-time | Renewable' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen | Non-Bedrock M2M | One-time' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen | One-time' then 'Hybrid'
        when CONTRACT_TYPE = 'Evergreen' then 'Monthly'
        when CONTRACT_TYPE = 'Evergreen | Non-Bedrock M2M' then 'Monthly'
        when CONTRACT_TYPE = 'Non-Bedrock M2M' then 'None'
        when CONTRACT_TYPE = '0' then 'None'
        when CONTRACT_TYPE = 'Non-Bedrock M2M | One-time' then 'One-time'
        when CONTRACT_TYPE = 'One-time' then 'One-time'
    end as contract_mapping,
    iff(contract_mapping = 'None', 1, 0) as Contract_Available,
    case
        when contract_mapping = 'Contracted' then 1
        when contract_mapping = 'Monthly' then 1
        when contract_mapping = 'One-Time' then 0
        when contract_mapping = 'Hybrid' then 0
        when contract_mapping = 'None' then 0
        else 0
    end as contract_points,
    ARR_UNIFIED_sum,
    last_year_arr,
    (ARR_UNIFIED_sum - last_year_arr) / nullifzero(last_year_arr) as arr_change,
    --                 case
    --                     when ARR_UNIFIED_sum is null and last_year_arr is null then 0
    --                     when arr_change is null then 1
    --                     else 0
    --                         end as ARR_change_available,
    iff(arr_change is null, 1, 0) as ARR_change_available,
    case
        when "Gainsight Risk" = 'A-B' then 2
        when "Gainsight Risk" = 'C' then 1
        when "Gainsight Risk" = 'D-F' then -1
        when "Gainsight Risk" = 'None' then 0
        else 0
    end as "Gainsight Risk Points",
    "Gainsight Score Available",
    17 - (
        iff(Contract_Available = 1, 1, 0) + iff(ARR_change_available = 1, 2, 0) + iff(
            "Gainsight Score Available" = 1,
            max("Gainsight Risk Points"),
            0
        )
    ) as max_available_score,
    case
        when arr_change is null then 0
        when arr_change >=.1 then 2
        when arr_change < -.1 then 0
        else 1
    end as previous_contracts,
    case
        when "Deal Value" = 'Tech Touch' then 3
        when "Deal Value" = 'Low Touch' then 2
        when "Deal Value" = 'High Touch' then 1
        else 0
    end as deal_value_points,
    case
        when (
            SECURITY_ACTIVE_PARTNER > 0
            and FORTIFY_ACTIVE_PARTNER > 0
            and BRIGHTGAUGE_ACTIVE_PARTNER > 0
        ) then 'Best'
        when (
            SECURITY_ACTIVE_PARTNER > 0
            or FORTIFY_ACTIVE_PARTNER > 0
        )
        and (
            SECURITY_ACTIVE_PARTNER = 0
            or FORTIFY_ACTIVE_PARTNER = 0
        ) then 'Good'
        when (
            SECURITY_ACTIVE_PARTNER > 0
            and FORTIFY_ACTIVE_PARTNER > 0
        ) then 'Better'
        else 'None'
    end as security_package,
    case
        when security_package = 'Good' then.36
        when security_package = 'Better' then.99
        when security_package = 'Best' then.99
        else 0
    end as sc_package_value,
    case
        when (
            SECURITY_ACTIVE_PARTNER > 0
            or FORTIFY_ACTIVE_PARTNER > 0
        ) then.99
        else 0
    end as fortify_security_value,
    case
        when (
            security_package <> 'None'
            or BRIGHTGAUGE_ACTIVE_PARTNER > 0
        ) then.45
        else 0
    end as sc_bg_value,
    case
        when (
            security_package <> 'None'
            or ITBOOST_ACTIVE_PARTNER > 0
        ) then.18
        else 0
    end as sc_it_value,
    (
        fortify_security_value + sc_bg_value + sc_it_value
    ) as current_value_2,
    (sc_package_value - current_value_2) as sc_value_add,
    case
        when AUTOMATE_ACTIVE_PARTNER > 0 then.41
        else 0
    end as automate_value,
    case
        when COMMAND_ACTIVE_PARTNER > 0 then.2
        else 0
    end as command_value,
    case
        when BRIGHTGAUGE_ACTIVE_PARTNER > 0 then.28
        else 0
    end as RMM_IT_value,
    case
        when ITBOOST_ACTIVE_PARTNER > 0 then.11
        else 0
    end as RMM_BG_value,
    (
        automate_value + command_value + RMM_IT_value + RMM_BG_value
    ) as current_value_1,
    case
        when "RMM Package" = 'Best' then 1.00
        when "RMM Package" = 'Better' then.80
        when "RMM Package" = 'Good' then.41
        else 0
    end as rmm_package_value,
    iff(
        rmm_package_value - current_value_1 < 0,
        0,
        rmm_package_value - current_value_1
    ) as rmm_value_add,
    case
        when "PSA Package Active Use FINAL" = 'Best' then 1.00
        when "PSA Package Active Use FINAL" = 'Better' then.87
        when "PSA Package Active Use FINAL" = 'Good' then.41
        else 0
    end as psa_package_value,
    case
        when MANAGE_ACTIVE_PARTNER > 0 then.408188060838409
        else 0
    end as manage_value,
    case
        when SELL_ACTIVE_PARTNER > 0 then.13131634937289
        else 0
    end as sell_value,
    case
        when (
            ITBOOST_ACTIVE_PARTNER > 0
            and psa_package_value is null
        ) then 0.0944352156335984
        else 0
    end as psa_it_value,
    case
        when (
            BRIGHTGAUGE_ACTIVE_PARTNER > 0
            and psa_package_value is null
        ) then.366060374155103
        else 0
    end as psa_bg_value,
    (
        manage_value + sell_value + psa_it_value + psa_bg_value
    ) as current_psa_value,
    (psa_package_value - current_psa_value) as psa_value_add,
    iff(
        sc_value_add + rmm_value_add + psa_value_add > 0,
        sc_value_add + rmm_value_add + psa_value_add,
        0
    ) as total_value_add,
    case

        when (
            total_value_add < 0
            or total_value_add > 1
        ) then 0
        when total_value_add between 0
        and.25 then 0
        when total_value_add between.26
        and.5 then 1
        when total_value_add between.51
        and 1 then -1
        else 0
    end as migration_value_add,
    (3) -(
        iff("PSA Package Active Use FINAL" = 'None', 1, 0) + iff("RMM Package" = 'NA', 1, 0) + iff(security_package = 'None', 1, 0)
    ) as number_of_modules_used,
    case
        when number_of_modules_used = 3 then 2
        when number_of_modules_used = 2 then 1
        when number_of_modules_used = 1 then 0
        else 0
    end as current_product_usage_tenure_poiints,
    iff("IT Nation" = 'Active Member', 1, 0) as peer_group_membership,
    case
        when "On-Prem/Hybrid" = 0 then 1
        else 0
    end as on_prem_risk,
    iff(
        customer_tenure_points + contract_points + previous_contracts + deal_value_points + migration_value_add + current_product_usage_tenure_poiints + peer_group_membership + "Gainsight Risk Points" + on_prem_risk > 0,
        customer_tenure_points + contract_points + previous_contracts + deal_value_points + migration_value_add + current_product_usage_tenure_poiints + peer_group_membership + "Gainsight Risk Points" + on_prem_risk,
        0
    ) as raw_risk,
    round((raw_risk / max_available_score) * 15, 0) as final_score,
    case
        when final_score between 0
        and 5 then 'High'
        when final_score between 6
        and 7 then 'Moderate'
        when final_score between 8
        and 9 then 'Low'
        when final_score >= 10 then 'Limited'
    end as risk_level,
    concat(risk_level, ' ', '-', ' ', final_score) as Risk_to_migrate
from
    final_table f
    left join "Current ARR All Categories" arr_cat on f.COMPANY_NAME_WITH_ID = arr_cat.COMPANY_NAME_WITH_ID
    left join last_year_arr lyarr on lyarr.COMPANY_NAME_WITH_ID = f.COMPANY_NAME_WITH_ID
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
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    52,
    53,
    54,
    55,
    56,
    57,
    58,
    59,
    60,
    61,
    62,
    63,
    64,
    65,
    66,
    67,
    68,
    69,
    70,
    71,
    72,
    73,
    74,
    75,
    76,
    77,
    78,
    79,
    80,
    81,
    82,
    83,
    84,
    85,
    86,
    87,
    88,
    89,
    90,
    91,
    92,
    93,
    94,
    95,
    96,
    97,
    98,
    99,
    100,
    101,
    102,
    103,
    104,
    105,
    106,
    107,
    108,
    109,
    110,
    111,
    112,
    113,
    114,
    115,
    116,
    117,
    118,
    119,
    120,
    121,
    122,
    123,
    124,
    125,
    126,
    127,
    128,
    129,
    130,
    131,
    132,
    133,
    134,
    135,
    136,
    137,
    138,
    arr_unified_sum,
    LAST_YEAR_ARR,
    RUN_DATE
  
