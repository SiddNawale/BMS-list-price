WITH deal_info_a AS (
    SELECT
        *,
        'USD' AS dummy_iso_code
    FROM
        analytics.dbo.core__rpt_bookings
    WHERE
        closedate <= CURRENT_DATE()
        AND closedate IS NOT NULL
        AND oppstatus IN (
            'Closed Won',
            'Closed Lost'
        )
),
bms_deals_a AS (
    SELECT
        DISTINCT opportunity_recid
    FROM
        deal_info_a
    WHERE
        UPPER(item_description) LIKE '%BUSINESS MANAGEMENT%'
),
final_win_status_a AS (
    SELECT
        bill_to AS max_won_ACCOUNT_ID,
        'Won' AS "account_final_status",
        MAX(closedate) AS close_date_max_won
    FROM
        deal_info_a
    WHERE
        oppstatus = 'Closed Won'
        AND UPPER(item_description) LIKE '%BUSINESS MANAGEMENT%'
    GROUP BY
        1
),
sale_hier AS (
    SELECT
        "Manager",
        rsm,
        "Fiscal_Year",
        CONCAT(
            "Fiscal_Year",
            '-',
            "Fiscal_Quarter",
            '-',
            "Fiscal_Month"
        ) AS new_date,
        SPLIT_PART(
            "RSM",
            ', ',
            1
        ) AS A,
        SPLIT_PART(
            "RSM",
            ', ',
            -1
        ) AS b,
        CASE
            WHEN A = b THEN A
            ELSE CONCAT(
                "B",
                ' ',
                "A"
            )
        END AS new_rsm_name
    FROM
        cwwebapp_reporting.dbo.refpositions
    WHERE
        "Team" IN (
            '1) Acquisition Sales',
            '2) Expansion Sales',
            '3) Growth Sales - AM',
            '4) Growth Sales - SEC'
        )
        AND new_date <= CURRENT_DATE() qualify ROW_NUMBER() over (
            PARTITION BY rsm
            ORDER BY
                new_date DESC
        ) = 1
)
SELECT
    deal_info_a.*,
    u.name,
    u.cws_team_group_c,
    bmsmap.product_name_ui,
    IFF(
        "LP_calc_flag" = 1,
        "Tiered Price Monthly",
        bmsmap."LP_USD"
    ) AS tiered_pricing,
    IFF(
        "LP_calc_flag" = 1,
        "BASE PRICE Monthly",
        bmsmap."LP_USD"
    ) AS list_price_static,
    CONCAT(
        bmsp."LOWERBOUND",
        ' to ',
        ROUND(
            bmsp."UPPERBOUND" -1
        )
    ) AS seat_tier,
    bmsp."LOWERBOUND",
    bmsp."UPPERBOUND",
    bmsmap."LP_calc_flag",
    bmsmap."LP_USD",
    s.new_rsm_name,
    s."Manager",
    s.new_date,
    s."RSM",
    ROW_NUMBER() over (
        PARTITION BY ship_to,
        item_id,
        oppstatus
        ORDER BY
            closedate
    ) AS ROW_NUMBER
FROM
    deal_info_a
    INNER JOIN bms_deals_a
    ON deal_info_a.opportunity_recid = bms_deals_a.opportunity_recid
    LEFT JOIN dataiku.dev_dataiku_staging.bms_productmapping bmsmap
    ON item_id = bmsmap.product_code
    LEFT JOIN dataiku.dev_dataiku_staging.usd_sku_information bmsp
    ON item_id = bmsp.productcode
    AND quantity >= bmsp.lowerbound
    AND quantity < bmsp.upperbound
    LEFT JOIN analytics.dbo_transformation.base_salesforce__user u
    ON deal_info_a.cws_account_manager_c = u.id
    LEFT JOIN sale_hier s
    ON u.name = s.new_rsm_name
