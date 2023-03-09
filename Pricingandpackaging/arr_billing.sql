with base as (
    SELECT
        COMPANY_ID,
        obt.company_name,
        reporting_date,
        PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT as Product --Renamed Column
,
        iff(
            Product_categorization_product_package = '🙈 nothing to see here',
            null,
            Product_categorization_product_package
        ) as Package --Renamed Column and changed nothing to see here to null
,
        iff(
            ITEM_Description = '🙈 nothing to see here',
            null,
            ITEM_Description
        ) as "Item Description" --Renamed Column and changed nothing to see here to null
,
        Item_ID,
        "Brand",
        seat_count."Include in Current ARR calculation",
        case
            when seat_count."Include in Current ARR calculation" = 1 then 'Include in Current ARR calculation'
            when seat_count."Include in Current ARR calculation" = 0 then 'Exclude in Current ARR calculation'
            else null
        end as "Seat Type",
        case
            when seat_count."Include in Seat Count" = 1 then 'Include in Unit Count'
            when seat_count."Include in Seat Count" = 0 then 'Exclude in Unit Count'
        end as units_include_flag,
        CASE
            -- !! A.H., Jan, 2023: The logic below needs to be updated down the road
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops', 'Networks', 'Servers')
                AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Third Party Patching')
                AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID = '0016g00001bbIz0AAE'
                AND MRR > 0
            ) THEN 'RMM'
            ELSE NULL
        END AS Category,
        currency_id as actual_currency,
        iff(
            sf.currency_iso_code = 'EUR',
            'USD',
            sf.currency_iso_code
        ) as reference_currency,
        exch.exchange_rate,
        u.NAME as Account_Manager,
        max(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
                and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                'Cloud',
                'OnPrem'
            )
        ) as Manage_service_type,
        max(
            iff(
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell'
                and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                'Cloud',
                'OnPrem'
            )
        ) as Sell_service_type,
        (SUM(BILLINGS)) AS Billings,
        (SUM(cast(ARR as int))) AS ARR -- casted ARR as int
,
        (SUM(billings_local * exch.exchange_rate) * 12) AS ARR_Unified,
        (SUM(UNITS)) AS UNITS,
        (SUM(BILLINGS_Including_Credit_Risk)) AS "BILLINGS INCLUDING CREDIT RISK",
        (SUM(cast(MRR as int))) AS MRR -- casted MRR as Int
,
        (SUM(Billings_local)) AS "BILLINGS LOCAL",
        (
            SUM(
                iff(
                    exch.EXCHANGE_RATE is not null,
                    Billings_local * exch.exchange_rate,
                    Billings_local
                )
            )
        ) as BillingsLocalUnified,
        case
            when reference_currency is not null
            and REPORTING_DATE is not null then cast(
                concat(reference_currency, ',', REPORTING_DATE) as string
            )
            when reference_currency is null
            and REPORTING_DATE is not null then cast(REPORTING_DATE as varchar)
        end as key
    FROM
        ANALYTICS.DBO.GROWTH__OBT as obt
        left join ANALYTICS.dbo_transformation.base_salesforce__account as sf on upper(sf.id) = upper(obt.company_id)
        and sf.name = obt.company_name
        left join analytics.dbo.cw_dw__exchange_rates_push_prior_month as exch on currency_id = exch.FROM_CURRENCY
        and iff(
            sf.currency_iso_code = 'EUR',
            'USD',
            sf.currency_iso_code
        ) = exch.to_currency
        and reporting_date = exch.DATE_EFFECTIVE
        LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__ACCOUNT a ON obt.COMPANY_ID = a.ID
        LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER u ON a.OWNER_ID = u.ID
        LEFT outer JOIN DATAIKU.DEV_DATAIKU_STAGING.MANAGE_SEAT_COUNT_ARR_STAGING seat_count -- merged manage seat count table
        on obt.ITEM_ID = seat_count."Product Code"
    WHERE
        1 = 1
        AND METRIC_OBJECT = 'applied_billings'
        and REPORTING_DATE <= (
            select
                distinct REPORTING_DATE
            from
                DATAIKU.PRD_DATAIKU_WRITE.REPORTING_DATE_LIMIT
        )
        and reporting_date > '2020-10-01'
        AND Datediff(month, reporting_date, cast(getdate() AS DATE)) >= 0
        AND Datediff(month, reporting_date, cast(getdate() AS DATE)) <= 36
        and REPORTING_DATE < cast(getdate() AS DATE) -- prevent future dates
    GROUP BY
        COMPANY_ID,
        obt.company_name,
        reporting_date,
        Product,
        Package,
        Category,
        "Item Description",
        Item_Id,
        currency_id,
        iff(
            sf.currency_iso_code = 'EUR',
            'USD',
            sf.currency_iso_code
        ),
        exch.exchange_rate,
        u.NAME,
        "Brand",
        "Include in Current ARR calculation",
        "Include in Seat Count",
        "Product Code"
    HAVING
        sum(billings) > 0
)
select
    company_id,
    company_name,
    reporting_date,
    product,
    package,
    "Item Description",
    item_id,
    "Brand",
    case
        when "Brand" = 'Manage' then 'Bus Mgmt'
        when "Brand" = 'Automate' then 'RMM'
        when "Brand" = 'Brightgauge' then 'Bus Mgmt'
        else category
    end as Category,
    -- added the correct category bm then renamed it to just category
    "Seat Type",
    units_include_flag,
    actual_currency,
    reference_currency,
    exchange_rate,
    account_manager,
    manage_service_type,
    sell_service_type,
    billings,
    arr,
    arr_unified,
    units,
    "BILLINGS INCLUDING CREDIT RISK",
    mrr,
    "BILLINGS LOCAL",
    "Include in Current ARR calculation",
    billingslocalunified,
    key
from
    base