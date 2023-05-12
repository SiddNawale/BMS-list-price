-- Project      No direct Project - Adhoc Request from Ciaran
-- Purpose      Used to get endpoints for RMM (Automate, Command, and CW RMM)
-- Jira issue   https://jira.connectwisedev.com/browse/PG-592
-- Input        - Tables:
--                      -- ANALYTICS.DBO.CORE__RPT_BILLINGS
-- Database     - Snowflake
-----------------------------------------------------------------------------------------------------
select
    distinct company_name,
    ship_to,
    UOM,
    MAX(
        IFF(
            PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate'
            and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Automate'
            and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-', 'Standard', 'Internal IT')
            and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in (
                'SaaS',
                'Maintenance',
                'On Premise (Subscription)'
            )
            and UOM = 'Endpoints',
            UNITS,
            0
        )
    ) AS AUTOMATE_UNITS,
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
            AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ilike ('%command%')
            and UOM = 'Endpoints',
            UNITS,
            0
        )
    ) as COMMAND_TOTAL_UNITS,
    sum(
        IFF(
            PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'CW RMM'
            and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'CW RMM'
            and UOM = 'Endpoints',
            UNITS,
            0
        )
    ) as cw_rmm_total_units
from
    ANALYTICS.DBO.CORE__RPT_BILLINGS
where
    APPLIED_DATE = '2023-04-01 00:00:00.000000000'
    and UOM = 'Endpoints'
group by
    1,
    2,
    3
having
    sum(CYCLE_AMOUNT) > 0
