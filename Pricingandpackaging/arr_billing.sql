with base as (
    SELECT
        COMPANY_ID,
        obt.company_name,
        COMPANY_NAME_WITH_ID,
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
            when seat_count."Include in Current ARR calculation" = 3 then 'Include in BMS/RMM ARR calculation'
            when seat_count."Include in Current ARR calculation" = 2 then 'Include in RMM ARR calculation'
            when seat_count."Include in Current ARR calculation" = 1 then 'Include in BMS ARR calculation'
            when seat_count."Include in Current ARR calculation" = 0 then 'Exclude from ARR Calculation'
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
                --AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge'
                --AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost'
                --AND ARR > 0
            ) THEN 'Bus Mgmt'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops', 'Networks', 'Servers')
                --AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate')
                AND PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Third Party Patching')
                --AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk'
                --AND ARR > 0
            ) THEN 'RMM'
            WHEN (
                lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot'
                AND ITEM_ID = '0016g00001bbIz0AAE'
                --AND MRR > 0
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
        (SUM(cast(MRR as int))) AS MRR,-- casted MRR as Int

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
    FROM ANALYTICS.DBO.GROWTH__OBT as obt
        left join ANALYTICS.dbo_transformation.base_salesforce__account as sf on sf.id = obt.company_id
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
        LEFT outer JOIN DATAIKU.DEV_DATAIKU_STAGING.PNP_DASHBOARD_MANAGE_SEATCOUNT_ARR seat_count -- merged manage seat count table
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
        "Product Code",
        COMPANY_NAME_WITH_ID
    HAVING
        sum(billings) > 0
)
select
       distinct
    company_id,
    company_name,
    base.COMPANY_NAME_WITH_ID,
    base.reporting_date,
    product,
    package,
    "Item Description",
    base.ITEM_ID,
    iff(rmm."Command SKU ID" = base.ITEM_ID,"RMM SKU ID",null) as RMM_HD_Mapping,
    "RMM Mapping",
    iff(rmm."Command SKU ID" = base.ITEM_ID,"RMM Name",null) as RMM_HD_Description,
    iff(NOC."Command SKU ID" = base.ITEM_ID,"NOC SKU ID",null) as NOC_SKU_Mapping,
    "NOC Mapping",
    iff(NOC."Command SKU ID" = base.ITEM_ID,"NOC Name",null) as NOC_Description,
    iff("Item Description" in ('ConnectWise Command Servers Elite' , 'ConnectWise Command Servers Preferred'), 1, null) noc_flag,
    iff(noc_flag = 1, UNITS,null)   as noc_units,
    noc_price.UNIT_PRICE as NOC_List_Price,
    rmm_price.UNIT_PRICE as HD_unit_price,
    (MRR/nullifzero(UNITS)) as price_per_seat,
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
    essential_price_per_seat,
    iff(base.ITEM_ID in ('CULCSAS100301ELITEB'), (price_per_seat-essential_price_per_seat), null) as noc_component_price, --Use Item_id instead of item description for iff statements
    desktop_essential_price_per_seat,
    iff("RMM Mapping" = 1, (price_per_seat-desktop_essential_price_per_seat), null) as HD_component_price,
    (HD_component_price*units) as HD_total_monthly_future_price_by_sku,
    (noc_units*noc_component_price) as noc_future_monthly_price,
    (HD_total_monthly_future_price_by_sku*12) as HD_total_annual_future_price_by_sku,
    (noc_future_monthly_price*12) as noc_future_annual_price

from
    base
        left join (select distinct * from DATAIKU.DEV_DATAIKU_WRITE.PNP_DASHBOARD_MANAGE_SEATCOUNT_RMM_MAPPING) rmm on base.ITEM_ID = rmm."Command SKU ID"
        left join (select distinct * from DATAIKU.DEV_DATAIKU_WRITE.PNP_DASHBOARD_MANAGE_SEATCOUNT_NOC_MAPPING) NOC on base.ITEM_ID = NOC."Command SKU ID"
        left join  fivetran.salesforce.pricebook_entry rmm_price on rmm_price.PRODUCT_CODE = base.ITEM_ID and reference_currency = rmm_price.CURRENCY_ISO_CODE and "RMM Mapping" = 1
        left join  fivetran.salesforce.pricebook_entry noc_price on noc_price.PRODUCT_CODE = noc."NOC SKU ID" and reference_currency = noc_price.CURRENCY_ISO_CODE and "NOC Mapping" = 1
        left join (select distinct COMPANY_NAME_WITH_ID, REPORTING_DATE, item_id,(MRR/nullifzero(UNITS)) as essential_price_per_seat
                    from base
                    where ITEM_ID in ('CULCSAS100101ESSENTB')) essential on essential.COMPANY_NAME_WITH_ID = base.COMPANY_NAME_WITH_ID and essential.REPORTING_DATE = base.REPORTING_DATE and base.ITEM_ID = 'CULCSAS100301ELITEB'

        left join (select distinct COMPANY_NAME_WITH_ID, REPORTING_DATE,(MRR)/nullifzero(UNITS) as desktop_essential_price_per_seat
                   from base
                   where ITEM_ID in ('CULCSAS1004010101190')) desktop_essential on desktop_essential.COMPANY_NAME_WITH_ID = base.COMPANY_NAME_WITH_ID and desktop_essential.REPORTING_DATE = base.REPORTING_DATE and "RMM Mapping" = 1