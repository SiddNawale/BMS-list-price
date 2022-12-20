WITH distinctids AS (
    SELECT
        ROW_NUMBER() over (
            PARTITION BY LTRIM(RTRIM(company_id))
            ORDER BY
                reporting_date DESC) AS row_num,
                LTRIM(RTRIM(company_id)) AS company_id,
                MAX(reporting_date)
            FROM
                analytics.dbo.growth__obt
            WHERE
                1 = 1
                AND company_name != ''
                AND metric_object = 'applied_billings'
            GROUP BY
                LTRIM(RTRIM(company_id)),
                reporting_date),
                distinctcompany AS (
                    SELECT
                        company_name,
                        ROW_NUMBER() over (
                            PARTITION BY LTRIM(RTRIM(company_id))
                            ORDER BY
                                reporting_date DESC) AS row_num,
                                LTRIM(RTRIM(company_id)) AS company_id,
                                MAX(reporting_date)
                            FROM
                                analytics.dbo.growth__obt
                            WHERE
                                1 = 1
                                AND company_name != ''
                                AND metric_object = 'applied_billings'
                            GROUP BY
                                LTRIM(RTRIM(company_id)),
                                reporting_date,
                                company_name),
                                final_company_information AS (
                                    SELECT
                                        distinctids.company_id,
                                        distinctcompany.company_name
                                    FROM
                                        distinctids
                                        INNER JOIN distinctcompany
                                        ON distinctcompany.company_id = distinctids.company_id
                                        AND distinctcompany.row_num = distinctids.row_num
                                    WHERE
                                        distinctids.row_num = 1
                                )
                            SELECT
                                DISTINCT company_id,
                                company_name,
                                UPPER(company_name) AS company_name_upper,
                                CONCAT(
                                    company_name,
                                    company_id
                                ) AS company_name_id
                            FROM
                                final_company_information
