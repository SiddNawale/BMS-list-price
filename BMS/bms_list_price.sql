WITH base AS (
    SELECT
        DISTINCT product_code AS product_code,
        NAME AS product_name,
        unit_price,
        cws_cost_price_c,
        currency_iso_code,
        IFF(
            product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            unit_price
        ) AS lp_usd,
        IFF(
            product_code IN (
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            0
        ) AS discount,
        IFF(
            product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            0
        ) AS units,
        IFF(
            product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            0
        ) AS acv,
        IFF(
            product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            0
        ) AS recurring,
        IFF(
            product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ),
            1,
            0
        ) AS flag,
        CASE
            WHEN product_code IN (
                'CW-BMS-STNDUSR-IMPLT',
                'CW-BMS-ADV-USR-IMPLT',
                'CW-BMS-USER-IMPLENT',
                'CW-BMS-CORE-IMPLENT'
            ) THEN 2
            WHEN product_code IN (
                'BMS-ADVANCED-BUNDLE',
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED',
                'CW-BMS-CORE',
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ) THEN 1
            ELSE 0
        END AS lp_calc_flag,
        CASE
            WHEN product_code IN (
                'BMS-ADVANCED-VERSION',
                'CW-BMS-ADVANCED'
            ) THEN 'Advanced'
            WHEN product_code IN (
                'BMS-ADVANCED-BUNDLE'
            ) THEN 'Advanced Bundle'
            WHEN product_code IN (
                'CW-BMS-ADV-USR-IMPLT',
                'CW-BMS-IMPLEMENT-ADV'
            ) THEN 'Advanced Implementation'
            WHEN product_code IN (
                'CW-BMS-IMPLEMENTADVN'
            ) THEN 'Advanced Implementation Billing Only'
            WHEN product_code IN (
                'CW-BMS-CORE'
            ) THEN 'Core'
            WHEN product_code IN (
                'CW-BMS-USER-IMPLENT',
                'CW-BMS-CORE-IMPLENT'
            ) THEN 'Core Implementation'
            WHEN product_code IN (
                'BMS-STANDARD-VERSION',
                'CW-BMS-STANDARD'
            ) THEN 'Standard'
            WHEN product_code IN (
                'CW-BMS-STNDUSR-IMPLT',
                'CW-BMS-IMPLEMNT-STND'
            ) THEN 'Standard Implementation'
            WHEN product_code IN (
                'CW-BMS-IMPLEMENTSTND'
            ) THEN 'Standard Implementation Billing Only'
            ELSE 'others'
        END AS product_name_ui,
        CASE
            WHEN product_code IN (
                'CW-BMS-ADVANCED',
                'BMS-ADVANCED-VERSION',
                'BMS-ADVANCED-BUNDLE',
                'CW-BMS-IMPLEMENTADVN',
                'CW-BMS-IMPLEMENT-ADV',
                'CW-BMS-ADV-USR-IMPLT'
            ) THEN 'Advanced'
            WHEN product_code IN (
                'CW-BMS-CORE',
                'CW-BMS-USER-IMPLENT',
                'CW-BMS-CORE-IMPLENT'
            ) THEN 'Core'
            WHEN product_code IN (
                'CW-BMS-STANDARD',
                'BMS-STANDARD-VERSION',
                'CW-BMS-IMPLEMENTSTND',
                'CW-BMS-IMPLEMNT-STND',
                'CW-BMS-STNDUSR-IMPLT'
            ) THEN 'Standard'
            ELSE 'others'
        END AS product_category
    FROM
        fivetran.salesforce.pricebook_entry
    WHERE
        currency_iso_code = 'USD'
        AND is_active = 'true'
        AND product_code IN (
            'CW-BMS-STNDUSR-IMPLT',
            'CW-BMS-ADV-USR-IMPLT',
            'CW-BMS-IMPLEMENT-ADV',
            'CW-BMS-IMPLEMENTADVN',
            'CW-BMS-IMPLEMENTSTND',
            'CW-BMS-IMPLEMNT-STND',
            'BMS-ADVANCED-BUNDLE',
            'BMS-ADVANCED-VERSION',
            'CW-BMS-ADVANCED',
            'CW-BMS-CORE',
            'BMS-STANDARD-VERSION',
            'CW-BMS-STANDARD',
            'CW-BMS-IMPLEMENTSTND',
            'CW-BMS-USER-IMPLENT',
            'CW-BMS-CORE-IMPLENT'
        )
)
SELECT
    product_name,
    product_code,
    product_category,
    product_name_ui,
    flag,
    recurring,
    acv,
    units,
    discount,
    lp_calc_flag,
    lp_usd
FROM
    base