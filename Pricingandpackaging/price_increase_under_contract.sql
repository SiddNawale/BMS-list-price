WITH active_contracts AS (
    SELECT
        con.contract_number,
        ROW_NUMBER() over (
            PARTITION BY contract_number,
            pe.product_code
            ORDER BY
                con.start_date
        ) AS contract_addition,
        con.start_date :: DATE AS contract_start_date,
        con.end_date :: DATE AS contract_end_date,
        A.id AS company_id,
        q.cws_route_to_market_c AS channel,
        con.cws_contract_acv_c AS contract_acv,
        sub.currency_iso_code,
        sub.sbqq_subscription_type_c AS subscription_type,
        sub.sbqq_subscription_start_date_c :: DATE AS product_start_date,
        sub.sbqq_subscription_end_date_c :: DATE AS product_end_date,
        sub.sbqq_quantity_c AS contracted_quantity,
        ROUND(
            div0(
                sub.sbqq_list_price_c,
                sub.cws_subscription_term_c
            ),
            2
        ) AS contract_List_Price,
        ROUND(
            div0(
                sub.sbqq_net_price_c,
                sub.cws_subscription_term_c
            ),
            2
        ) AS contract_sale_price,
        sub.cws_acvline_c AS product_acv,
        sub.sbqq_product_subscription_type_c AS product_subscription_type,
        sp.product_code,
        div0(
            pe.unit_price,
            sp.sbqq_subscription_term_c
        ) AS pricebook_price
    FROM
        fivetran.salesforce.contract con
        INNER JOIN fivetran.salesforce.account A
        ON con.account_id = A.id
        LEFT JOIN fivetran.salesforce.account bill_to
        ON con.cws_bill_to_account_c = bill_to.id
        LEFT JOIN fivetran.salesforce.sbqq_subscription_c sub
        ON con.id = sub.sbqq_contract_c
        LEFT JOIN fivetran.salesforce.product_2 sp
        ON sub.sbqq_product_c = sp.id
        LEFT JOIN fivetran.salesforce.pricebook_entry pe
        ON sp.id = pe.product_2_id
        AND pe.currency_iso_code = sub.currency_iso_code
        AND pe.pricebook_2_id = '01s6g00000A5vXTAAZ'
        LEFT JOIN fivetran.salesforce.user u
        ON A.owner_id = u.id
        LEFT JOIN fivetran.salesforce.sbqq_quote_line_c ql
        ON sub.sbqq_quote_line_c = ql.id
        LEFT JOIN fivetran.salesforce.sbqq_quote_c q
        ON ql.sbqq_quote_c = q.id
    WHERE
        con.cws_cancelled_c = FALSE
        AND con.start_date < CURRENT_DATE
        AND (
            con.end_date IS NULL
            OR con.end_date > CURRENT_DATE
        )
        AND (
            sub.sbqq_terminated_date_c IS NULL
            OR sub.sbqq_terminated_date_c > CURRENT_DATE
        )
),
FINAL AS (
    SELECT
        b.applied_date,
        A.cws_account_unique_identifier_c,
        b.ship_to,
        b.company_name,
        bu,
        lens,
        lens3,
        b.item_id,
        b.description AS item_description,
        im.product,
        im.product_tier,
        b.contract_number,
        contract_start_date,
        contract_end_date,
        ac.channel,
        ac.subscription_type,
        contracted_quantity,
        ac.currency_iso_code,
        contract_addition,
        product_start_date,
        product_end_date,
        pricebook_price,
        contract_List_Price,
        contract_sale_price,
        product_acv,
        SUM(
            IFF(
                contract_addition = 1
                OR contract_addition IS NULL,
                cycle_amount,
                0
            )
        ) AS billings,
        SUM(
            IFF(
                mrr_flag = 1
                AND (
                    contract_addition = 1
                    OR contract_addition IS NULL
                ),
                cycle_amount,
                0
            )
        ) AS mrr,
        SUM(
            IFF(
                mrr_flag = 1
                AND (
                    contract_addition = 1
                    OR contract_addition IS NULL
                ),
                cycle_amount,
                0
            ) * 12
        ) AS arr
    FROM
        analytics.dbo.core__rpt_billings b
        LEFT JOIN fivetran.salesforce.account A
        ON b.ship_to = A.id
        LEFT JOIN analytics.DBO_TRANSFORMATION.seed__item_mapping_for_product_team im
        ON im.item_id = b.item_id
        LEFT JOIN active_contracts ac
        ON b.applied_date >= ac.contract_start_date
        AND b.applied_date <= COALESCE(
            ac.contract_end_date,
            CURRENT_DATE
        )
        AND b.ship_to = ac.company_id
        AND b.item_id = ac.product_code
    WHERE
        b.applied_date = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE))
        AND b.applied_date < CURRENT_DATE
    GROUP BY
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
        25
)
SELECT
    *
FROM
    FINAL
