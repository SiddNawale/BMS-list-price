-- loading automate billings,
filtering
ON MAX DATE OF automate billings -- loading rmm billings,
filtering
ON MAX DATE OF rmm billings -- checking price
AND units difference OF partners
FROM
    automate TO cwrmm -- pull DATA OF running 12 months
FROM
    automate side AS base WITH rmm AS (
        SELECT
            obt.company_id,
            obt.reporting_date,
            plm."Product Name New" AS item_description,
            SUM(
                obt.units
            ) AS rmm_units,
            SUM(
                obt.mrr
            ) AS rmm_mrr,
            MAX(
                obt.reporting_date
            ) over(
                PARTITION BY company_id
            ) AS max_rmm_date,
            MIN(
                obt.reporting_date
            ) over(
                PARTITION BY company_id
            ) AS min_rmm_date
        FROM
            analytics.dbo.growth__obt obt
            LEFT JOIN dataiku.prd_dataiku_write."CW_RMM_POST_LAUNCH_PRODUCT_MAPPING" plm
            ON obt.item_id = plm.product_code
        WHERE
            metric_object = 'applied_billings'
            AND product_categorization_product_line = 'CW RMM' --
            AND mrr_flag = 1
            AND reporting_date <=(
                SELECT
                    DISTINCT CASE
                        WHEN DAY(CURRENT_DATE()) > 3 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                        ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                    )
                GROUP BY
                    1,
                    2,
                    3 --
                HAVING
                    -- SUM(units) > 0 --
                    OR SUM(mrr) > 0
            ),
            automate AS (
                SELECT
                    DISTINCT obt.company_id,
                    obt.reporting_date,
                    MAX(units) AS automate_units,
                    SUM(mrr) AS automate_mrr,
                    (
                        SELECT
                            DISTINCT CASE
                                WHEN DAY(CURRENT_DATE()) > 2 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                            ) AS check_date_automate
                        FROM
                            analytics.dbo.growth__obt obt
                        WHERE
                            metric_object = 'applied_billings'
                            AND product_categorization_product_line = 'Automate' --
                            AND product_categorization_product_package = 'Automate' --
                            AND product_categorization_license_service_type IN (
                                'SaaS',
                                'On Premise (Subscription)',
                                'Maintenance'
                            ) --
                            AND mrr_flag = 1
                            AND reporting_date <= (
                                SELECT
                                    DISTINCT CASE
                                        WHEN DAY(CURRENT_DATE()) > 3 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                        ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                    )
                                    AND reporting_date >= ('2019-01-01')
                                GROUP BY
                                    1,
                                    2
                            ),
                            max_date AS(
                                -- MAX OF automate DATE
                                SELECT
                                    company_id,
                                    MAX(reporting_date) AS rd
                                FROM
                                    automate
                                GROUP BY
                                    1
                            ),
                            automate_pre_migration_price AS (
                                -- less than MIN OF cwrmm DATE price
                                SELECT
                                    A.company_id,
                                    A.reporting_date,-- MAX(
                                        A.reporting_date
                                    ) AS pre_mig_max_automate,
                                    SUM(automate_mrr) AS pre_migration_automate_mrr,
                                    SUM(automate_mrr) * 12 AS pre_migration_automate_arr,
                                    SUM(automate_units) AS pre_migration_automate_units,
                                    ROW_NUMBER() over (
                                        PARTITION BY A.company_id
                                        ORDER BY
                                            A.reporting_date DESC
                                    ) AS pre_migration_month_number_desc,
                                    1 AS pre_migartion_flag
                                FROM
                                    automate A
                                    INNER JOIN (
                                        SELECT
                                            DISTINCT company_id,
                                            min_rmm_date
                                        FROM
                                            rmm
                                    ) rmm
                                    ON A.company_id = rmm.company_id
                                    AND A.reporting_date < rmm.min_rmm_date
                                GROUP BY
                                    1,
                                    2
                                HAVING
                                    SUM(automate_mrr) > 0
                            ),
                            automate_max_date AS (
                                SELECT
                                    automate.*,
                                    1 AS automate_flag
                                FROM
                                    automate
                                    INNER JOIN max_date
                                    ON automate.company_id = max_date.company_id
                                    AND automate.reporting_date = max_date.rd
                            ),
                            max_rmm AS (
                                SELECT
                                    company_id,
                                    MAX(reporting_date) AS rd
                                FROM
                                    rmm
                                GROUP BY
                                    1
                            ),
                            rmm_agg_prods AS (
                                SELECT
                                    rmm.company_id AS rmm_company_id,
                                    rmm.item_description,
                                    rmm.reporting_date AS rmm_reporting_date,
                                    rmm.min_rmm_date,
                                    1 AS rmm_flag,
                                    (
                                        SELECT
                                            DISTINCT CASE
                                                WHEN DAY(CURRENT_DATE()) > 3 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                                ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                            ) AS check_date_rmm,
                                            SUM(rmm_units) AS rmm_units,
                                            SUM(rmm_mrr) AS rmm_mrr
                                        FROM
                                            rmm
                                            INNER JOIN max_rmm
                                            ON rmm.company_id = max_rmm.company_id
                                            AND rmm.reporting_date = max_rmm.rd
                                        WHERE
                                            item_description IS NOT NULL
                                        GROUP BY
                                            1,
                                            2,
                                            3,
                                            4,
                                            5 --
                                        HAVING
                                            SUM(rmm_units) > 0
                                            OR SUM(rmm_mrr) > 0
                                    ),
                                    rmm_agg_others AS (
                                        SELECT
                                            rmm.company_id AS rmm_company_id,
                                            rmm.reporting_date AS rmm_others_reporting_date,
                                            1 AS rmm_other_flag,
                                            SUM(rmm_units) AS rmm_other_units,
                                            SUM(rmm_mrr) AS rmm__other_mrr,
                                            (
                                                SELECT
                                                    DISTINCT CASE
                                                        WHEN DAY(CURRENT_DATE()) > 3 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                                        ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                                    ) AS check_date_rmm_others
                                                FROM
                                                    rmm
                                                    INNER JOIN max_rmm
                                                    ON rmm.company_id = max_rmm.company_id
                                                    AND rmm.reporting_date = max_rmm.rd
                                                WHERE
                                                    item_description IS NULL
                                                GROUP BY
                                                    1,
                                                    2 --
                                                HAVING
                                                    SUM(rmm_units) > 0
                                                    OR SUM(rmm_mrr) > 0
                                            ),
                                            noc AS (
                                                SELECT
                                                    obt.company_id,
                                                    obt.reporting_date,
                                                    SUM(
                                                        IFF(
                                                            product_categorization_product_package = 'NOC'
                                                            AND mrr_flag = 1
                                                            AND product_categorization_product_plan = 'Elite',
                                                            units,
                                                            0
                                                        )
                                                    ) AS noc_server_units,
                                                    SUM(
                                                        IFF(
                                                            product_categorization_product_package = 'NOC'
                                                            AND mrr_flag = 1
                                                            AND product_categorization_product_plan != 'Elite',
                                                            units,
                                                            0
                                                        )
                                                    ) AS noc_desktop_units,
                                                    SUM(
                                                        IFF(
                                                            product_categorization_product_package = 'NOC'
                                                            AND mrr_flag = 1
                                                            AND product_categorization_product_plan = 'Elite',
                                                            mrr,
                                                            0
                                                        )
                                                    ) AS noc_server_mrr,
                                                    SUM(
                                                        IFF(
                                                            product_categorization_product_package = 'NOC'
                                                            AND mrr_flag = 1
                                                            AND product_categorization_product_plan != 'Elite',
                                                            mrr,
                                                            0
                                                        )
                                                    ) AS noc_desktop_mrr
                                                FROM
                                                    analytics.dbo.growth__obt obt
                                                WHERE
                                                    metric_object = 'applied_billings'
                                                    AND mrr_flag = 1
                                                    AND reporting_date <=(
                                                        SELECT
                                                            DISTINCT CASE
                                                                WHEN DAY(CURRENT_DATE()) > 3 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                                                ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                                            )
                                                        GROUP BY
                                                            1,
                                                            2
                                                    )
                                                SELECT
                                                    DISTINCT rmm.*,
                                                    amd.company_id,
                                                    amd.reporting_date AS automate_reporting_date,
                                                    automate_units,
                                                    automate_mrr,
                                                    rmm_other_units,
                                                    rmm__other_mrr,
                                                    rmm_other_flag,
                                                    automate_flag,
                                                    rmm_others_reporting_date,
                                                    CASE
                                                        -- rmm IS NULL,
                                                        automate
                                                        AND ONLY rmm other skus are active
                                                        WHEN rmm_reporting_date IS NULL
                                                        AND automate_reporting_date = check_date_rmm_others
                                                        AND automate_reporting_date = rmm_others_reporting_date THEN 'In RMM Implementation' -- rmm
                                                        AND rmm OTHERS IS NULL,
                                                        automate IS active = never moved TO rmm / still WITH automate
                                                        WHEN rmm_reporting_date IS NULL
                                                        AND rmm_others_reporting_date IS NULL
                                                        AND automate_reporting_date = check_date_automate THEN 'Still with Automate' -- rmm
                                                        AND rmm other IS NULL,
                                                        automate DATA IS NOT active DATE = churned automate
                                                        WHEN rmm_reporting_date IS NULL
                                                        AND rmm_others_reporting_date IS NULL
                                                        AND automate_reporting_date < check_date_automate THEN 'Churned Automate' -- rmm
                                                        AND automate are active = keeping BOTH
                                                        WHEN (
                                                            rmm_reporting_date = automate_reporting_date
                                                        )
                                                        AND (
                                                            rmm_reporting_date = check_date_rmm
                                                        ) THEN 'On RMM & Automate Billing' -- rmm
                                                        OR rmm OTHERS reporting DATE IS more than automate,
                                                        WHEN (
                                                            automate_reporting_date < check_date_automate
                                                        )
                                                        AND (
                                                            rmm_reporting_date = check_date_rmm
                                                            OR rmm_others_reporting_date = check_date_rmm_others
                                                        ) THEN 'Migrated to RMM' -- GREATEST DATE OF rmm
                                                        AND rmm OTHERS are NOT active,
                                                        automate IS active
                                                        WHEN GREATEST(
                                                            COALESCE(
                                                                rmm_reporting_date,
                                                                '2000-01-01'
                                                            ),
                                                            COALESCE(
                                                                rmm_others_reporting_date,
                                                                '2000-01-01'
                                                            )
                                                        ) < check_date_automate
                                                        AND automate_reporting_date = check_date_automate THEN 'Revert to Automate' -- GREATEST DATE OF BOTH rmm
                                                        AND rmm OTHERS are NOT active
                                                        AND GREATEST DATE OF rmms IS more than automate
                                                        WHEN (
                                                            GREATEST(
                                                                COALESCE(
                                                                    rmm_reporting_date,
                                                                    '2000-01-01'
                                                                ),
                                                                COALESCE(
                                                                    rmm_others_reporting_date,
                                                                    '2000-01-01'
                                                                )
                                                            ) > automate_reporting_date
                                                            AND GREATEST(
                                                                COALESCE(
                                                                    rmm_reporting_date,
                                                                    '2000-01-01'
                                                                ),
                                                                COALESCE(
                                                                    rmm_others_reporting_date,
                                                                    '2000-01-01'
                                                                )
                                                            ) < check_date_automate
                                                        ) THEN 'Churned RMM'
                                                        WHEN (
                                                            GREATEST(
                                                                COALESCE(
                                                                    rmm_reporting_date,
                                                                    '2000-02-01'
                                                                ),
                                                                COALESCE(
                                                                    rmm_others_reporting_date,
                                                                    '2000-01-01'
                                                                )
                                                            ) < automate_reporting_date
                                                            AND automate_reporting_date < check_date_automate
                                                            AND (
                                                                COALESCE(
                                                                    rmm_reporting_date,
                                                                    rmm_others_reporting_date
                                                                ) IS NOT NULL
                                                            )
                                                        ) THEN 'Revert and Churn Automate'
                                                        ELSE 'Others'
                                                    END AS rmm_status,
                                                    IFF(
                                                        automate_reporting_date = check_date_automate,
                                                        1,
                                                        0
                                                    ) AS active_automate,
                                                    rmm_units - automate_units AS units_difference,
                                                    rmm_mrr - automate_mrr AS mrr_difference,
                                                    (rmm_mrr) * 12 AS rmm_arr,
                                                    pmp.pre_migration_automate_mrr,
                                                    pmp.pre_migration_automate_arr,
                                                    pmp.pre_migration_automate_units,-- pre_mig_max_automate,
                                                    noc.noc_desktop_mrr,
                                                    noc.noc_desktop_units,
                                                    noc.noc_server_mrr,
                                                    noc.noc_server_units
                                                FROM
                                                    automate_max_date amd
                                                    LEFT JOIN rmm_agg_prods rmm
                                                    ON rmm.rmm_company_id = amd.company_id
                                                    LEFT JOIN rmm_agg_others rmmo
                                                    ON amd.company_id = rmmo.rmm_company_id
                                                    LEFT JOIN automate_pre_migration_price pmp
                                                    ON amd.company_id = pmp.company_id
                                                    LEFT JOIN noc
                                                    ON noc.company_id = rmm.rmm_company_id
                                                    AND noc.reporting_date = rmm.rmm_reporting_date
                                                    AND pmp.pre_migration_month_number_desc = 1
                                                WHERE
                                                    amd.company_id = '0016g00000pUolEAAS'
