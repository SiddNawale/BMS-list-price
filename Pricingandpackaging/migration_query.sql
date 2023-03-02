WITH customer_roster AS (
        SELECT
                reporting_date,-- company_name_with_id,
                RTRIM(LTRIM(company_id)) AS company_id,-- trimmed companyy id MIN(
                        IFF(
                                company_name = '🙈 nothing to see here',
                                NULL,
                                company_name
                        )
                ) AS company_name,-- replaced nothing TO see here WITH NULL MAX(
                        IFF(
                                product_categorization_product_line = 'Manage'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS manage_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Control'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS control_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Automate'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS automate_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Sell'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS sell_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Fortify'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS fortify_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Command'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS command_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'BrightGauge'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS brightgauge_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Recover'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS recover_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Help Desk'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS help_desk_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'ITBoost'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS itboost_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'Perch'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS security_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'IT Nation'
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS itnation_active_partner,
                MAX(
                        IFF(
                                product_categorization_product_line = 'IT Nation'
                                AND product_categorization_product_package IN (
                                        'Evolve',
                                        'ITN Evolve'
                                )
                                AND billings > 0,
                                1,
                                0
                        )
                ) AS itnation_peer_group_active_partner,
                MAX(
                        IFF(LOWER(product_categorization_product_package) = 'webroot'
                        AND item_id <> '3P-SAAS3002315EPPRMM', 1, 0)) AS webroot_active_partner,-- SUM(
                                IFF(
                                        product_categorization_product_line IN (
                                                'Automate',
                                                'Command'
                                        )
                                        AND -- product_categorization_product_package IN (
                                                'Automate',
                                                'Desktops',
                                                'Networks',
                                                'Servers'
                                        ),
                                        units,- - 0
                                )
                        ) AS rmm_units,
                        SUM(
                                IFF(
                                        item_id IN (
                                                'LEGACYLTASSURANCEANN',
                                                'LEGACYSKULAAGNTSA24',
                                                'LEGACYSKULPHTGAGENT2',
                                                'AULICONPREMPAGENTLEG',
                                                'AUT-LIC-SAASAGNTSA12',
                                                'AUT-LIC-SAASAGNTSA36',
                                                'AUT-LIC-SAASAGC36SAS',
                                                'AUT-LIC-SAASAGENTLGA',
                                                'LEGACYSKULBAGNTSM24',
                                                'AULICONPREMBAGNTSM36',
                                                'AULICONPREMBAGNTSB12',
                                                'AULICONPREMBAGNTSB36',
                                                'AULICONPREMPAGP12PUF',
                                                'AUTLICSAASAGNTS36D2',
                                                'AUTLICSAASAGNTS36D3',
                                                'AUMAINPRPINTLTAGNTAI',
                                                'AUMAINPRPADDLTAGNTAA',
                                                'AUMAINPRPADDSMBITAG1',
                                                'AUMAINPRPADDAGENTLEG',
                                                'AUMAINPRPADDHTGAGT01',
                                                'AUMAINPRPADDHTGAGT02',
                                                'AULICONPREMPHTGAGEN1',
                                                'AULICONPREMPHTGAGEN2',
                                                'AUTLICSAASAGC24SASAS',
                                                'AUT-LIC-SAASPKSNDSAS',
                                                'AULICONPREMBPKSNDSUB',
                                                'LEGACYLTDWWM-AGC6',
                                                'LEGACYLT-ASSURANCE',
                                                'LEGACYLTCHNLASURANCE',
                                                'LEGACYCHNASURANCEANN',
                                                'LEGACYLTNEWSAASAGENT',
                                                'LEGACYLT-SAAS-AGENT',
                                                'LEGACYLBAGENTOPSM14',
                                                'AU-LIC-SAAS-AGENTSAS',
                                                'LEGACYLTASSURANCEQTR'
                                        ),
                                        units,
                                        0
                                )
                        ) AS automate_units_calc,
                        SUM(
                                IFF(product_categorization_product_line IN ('Command')
                                AND product_categorization_product_package IN ('Desktops'), units, 0)) AS command_desktop_units,
                                SUM(
                                        IFF(product_categorization_product_line IN ('Command')
                                        AND product_categorization_product_package IN ('Networks'), units, 0)) AS command_network_units,
                                        SUM(
                                                IFF(product_categorization_product_line IN ('Command')
                                                AND product_categorization_product_package IN ('Servers'), units, 0)) AS command_server_units,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line IN ('Help Desk'),
                                                                units,
                                                                0
                                                        )
                                                ) AS help_desk_units,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_group = 'Network & Endpoint Security'
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                units,
                                                                0
                                                        )
                                                ) AS security_units,
                                                (SUM(billings)) AS current_billings,
                                                (SUM(arr)) AS current_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line = 'Sell',
                                                                arr,
                                                                0
                                                        )
                                                ) AS sell_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line = 'BrightGauge',
                                                                arr,
                                                                0
                                                        )
                                                ) AS brightgauge_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line = 'ITBoost',
                                                                arr,
                                                                0
                                                        )
                                                ) AS itboost_arr,
                                                SUM(
                                                        IFF(product_categorization_product_line IN ('Command')
                                                        AND product_categorization_product_package IN ('Desktops', 'Networks', 'Servers'), arr, 0)
                                                ) AS command_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line IN ('Automate')
                                                                AND product_categorization_product_package IN ('Third Party Patching'),
                                                                arr,
                                                                0
                                                        )
                                                ) AS rmm_additional_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line = 'Help Desk',
                                                                arr,
                                                                0
                                                        )
                                                ) AS help_desk_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_group = 'Network & Endpoint Security'
                                                                AND product_categorization_license_service_type = 'SaaS'
                                                                AND LOWER(product_categorization_product_package) <> 'webroot',
                                                                arr,
                                                                0
                                                        )
                                                ) AS security_arr,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line NOT IN (
                                                                        'Sell',
                                                                        'BrightGauge',
                                                                        'ITBoost',
                                                                        'Help Desk'
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line = 'Manage'
                                                                        AND product_categorization_product_package = 'Manage'
                                                                        AND product_categorization_product_plan IN (
                                                                                '-',
                                                                                'Standard',
                                                                                'Premium',
                                                                                'Basic'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line IN (
                                                                                'Automate',
                                                                                'Command'
                                                                        )
                                                                        AND product_categorization_product_package IN (
                                                                                'Automate',
                                                                                'Desktops',
                                                                                'Networks',
                                                                                'Servers'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_group = 'Network & Endpoint Security'
                                                                        AND product_categorization_license_service_type = 'SaaS'
                                                                ),
                                                                arr,
                                                                0
                                                        )
                                                ) AS other_arr,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Sell'
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,
                                                                0
                                                        )
                                                ) AS sell_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'BrightGauge'
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,
                                                                0
                                                        )
                                                ) AS brightgauge_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'ITBoost'
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,
                                                                0
                                                        )
                                                ) AS itboost_cloud,-- MAX(
                                                        IFF(
                                                                product_categorization_product_line IN (
                                                                        'Automate',
                                                                        'Command'
                                                                )
                                                                AND -- product_categorization_product_package IN (
                                                                        'Automate',
                                                                        'Desktops',
                                                                        'Networks',
                                                                        'Servers'
                                                                ) --
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,- - 0
                                                        )
                                                ) AS rmm_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Help Desk',
                                                                1,
                                                                0
                                                        )
                                                ) AS help_desk_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_group = 'Network & Endpoint Security'
                                                                AND product_categorization_license_service_type = 'SaaS'
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,
                                                                0
                                                        )
                                                ) AS security_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line NOT IN (
                                                                        'Sell',
                                                                        'BrightGauge',
                                                                        'ITBoost',
                                                                        'Help Desk'
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line = 'Manage'
                                                                        AND product_categorization_product_package = 'Manage'
                                                                        AND product_categorization_product_plan IN (
                                                                                '-',
                                                                                'Standard',
                                                                                'Premium',
                                                                                'Basic'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line IN (
                                                                                'Automate',
                                                                                'Command'
                                                                        )
                                                                        AND product_categorization_product_package IN (
                                                                                'Automate',
                                                                                'Desktops',
                                                                                'Networks',
                                                                                'Servers'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_group = 'Network & Endpoint Security'
                                                                        AND product_categorization_license_service_type = 'SaaS'
                                                                )
                                                                AND product_categorization_license_service_type = 'SaaS',
                                                                1,
                                                                0
                                                        )
                                                ) AS other_cloud,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Sell'
                                                                AND product_categorization_license_service_type IN (
                                                                        'Perpetual',
                                                                        'Maintenance'
                                                                ),
                                                                arr,
                                                                0
                                                        )
                                                ) AS sell_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'BrightGauge'
                                                                AND product_categorization_license_service_type IN (
                                                                        'Perpetual',
                                                                        'Maintenance'
                                                                ),
                                                                arr,
                                                                0
                                                        )
                                                ) AS brightgauge_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'ITBoost'
                                                                AND product_categorization_license_service_type IN (
                                                                        'Perpetual',
                                                                        'Maintenance'
                                                                ),
                                                                arr,
                                                                0
                                                        )
                                                ) AS itboost_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Help Desk',
                                                                1,
                                                                0
                                                        )
                                                ) AS help_desk_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_group = 'Network & Endpoint Security'
                                                                AND product_categorization_license_service_type IN (
                                                                        'Perpetual',
                                                                        'Maintenance'
                                                                ),
                                                                1,
                                                                0
                                                        )
                                                ) AS security_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line NOT IN (
                                                                        'Sell',
                                                                        'BrightGauge',
                                                                        'ITBoost',
                                                                        'Help Desk'
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line = 'Manage'
                                                                        AND product_categorization_product_package = 'Manage'
                                                                        AND product_categorization_product_plan IN (
                                                                                '-',
                                                                                'Standard',
                                                                                'Premium',
                                                                                'Basic'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line IN (
                                                                                'Automate',
                                                                                'Command'
                                                                        )
                                                                        AND product_categorization_product_package IN (
                                                                                'Automate',
                                                                                'Desktops',
                                                                                'Networks',
                                                                                'Servers'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_group = 'Network & Endpoint Security'
                                                                        AND product_categorization_license_service_type = 'SaaS'
                                                                )
                                                                AND product_categorization_license_service_type IN (
                                                                        'Perpetual',
                                                                        'Maintenance'
                                                                ),
                                                                1,
                                                                0
                                                        )
                                                ) AS other_legacy_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Sell'
                                                                AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                                                arr,
                                                                0
                                                        )
                                                ) AS sell_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'BrightGauge'
                                                                AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                                                arr,
                                                                0
                                                        )
                                                ) AS brightgauge_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'ITBoost'
                                                                AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                                                arr,
                                                                0
                                                        )
                                                ) AS itboost_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line = 'Help Desk',
                                                                1,
                                                                0
                                                        )
                                                ) AS help_desk_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_group = 'Network & Endpoint Security'
                                                                AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                                                1,
                                                                0
                                                        )
                                                ) AS security_on_prem,
                                                MAX(
                                                        IFF(
                                                                product_categorization_product_line NOT IN (
                                                                        'Sell',
                                                                        'BrightGauge',
                                                                        'ITBoost',
                                                                        'Help Desk'
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line = 'Manage'
                                                                        AND product_categorization_product_package = 'Manage'
                                                                        AND product_categorization_product_plan IN (
                                                                                '-',
                                                                                'Standard',
                                                                                'Premium',
                                                                                'Basic'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_line IN (
                                                                                'Automate',
                                                                                'Command'
                                                                        )
                                                                        AND product_categorization_product_package IN (
                                                                                'Automate',
                                                                                'Desktops',
                                                                                'Networks',
                                                                                'Servers'
                                                                        )
                                                                )
                                                                AND NOT (
                                                                        product_categorization_product_group = 'Network & Endpoint Security'
                                                                        AND product_categorization_license_service_type = 'SaaS'
                                                                )
                                                                AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                                                1,
                                                                0
                                                        )
                                                ) AS other_on_prem,
                                                SUM(
                                                        IFF(
                                                                product_categorization_product_line LIKE '%Solution Partners%'
                                                                AND LOWER(product_categorization_product_package) <> 'webroot',
                                                                mrr,
                                                                0
                                                        )
                                                ) AS third_party_mrr,
                                                SUM(
                                                        IFF(LOWER(product_categorization_product_package) = 'webroot'
                                                        AND item_id <> '3P-SAAS3002315EPPRMM', mrr, 0)) AS webroot_mrr,
                                                        SUM(
                                                                IFF(LOWER(product_categorization_product_package) = 'webroot'
                                                                AND item_id <> '3P-SAAS3002315EPPRMM', units, 0)) AS webroot_units,
                                                                SUM(
                                                                        IFF(LOWER(product_categorization_product_package) = 'webroot'
                                                                        AND item_id = '3P-SAAS3002315EPPRMM', mrr, 0)) AS webroot_overage_mrr,
                                                                        SUM(
                                                                                IFF(LOWER(product_categorization_product_package) = 'webroot'
                                                                                AND item_id = '3P-SAAS3002315EPPRMM', units, 0)) AS webroot_overage_units
                                                                                FROM
                                                                                        analytics.dbo.growth__obt
                                                                                WHERE
                                                                                        reporting_date = (
                                                                                                SELECT
                                                                                                        DISTINCT CASE
                                                                                                                WHEN DAY(CURRENT_DATE()) > 2 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                                                                                                ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                                                                                        )
                                                                                                        AND metric_object = 'applied_billings'
                                                                                                        AND company_name <> ''
                                                                                                GROUP BY
                                                                                                        1,
                                                                                                        2
                                                                                                HAVING
                                                                                                        SUM(billings) > 0
                                                                                        ),
                                                                                        contract AS (
                                                                                                WITH fl AS (
                                                                                                        SELECT
                                                                                                                company_id,
                                                                                                                contract_number,
                                                                                                                product_categorization_arr_reported_product,
                                                                                                                MIN(start_date) AS start_date,
                                                                                                                MAX(end_date) AS end_date
                                                                                                        FROM
                                                                                                                analytics.dbo.growth__obt
                                                                                                        WHERE
                                                                                                                metric_object = 'renewals'
                                                                                                        GROUP BY
                                                                                                                1,
                                                                                                                2,
                                                                                                                3
                                                                                                ),
                                                                                                sl AS (
                                                                                                        SELECT
                                                                                                                *,
                                                                                                                IFF(
                                                                                                                        end_date < '2022-01-01' :: DATE,
                                                                                                                        '2099-01-01' :: DATE,
                                                                                                                        end_date
                                                                                                                ) AS dayfilter,
                                                                                                                MIN(dayfilter) over (
                                                                                                                        PARTITION BY company_id
                                                                                                                        ORDER BY
                                                                                                                                company_id
                                                                                                                ) AS nearestdate,
                                                                                                                IFF(
                                                                                                                        nearestdate = end_date,
                                                                                                                        1,
                                                                                                                        0
                                                                                                                ) AS daysfilterflag
                                                                                                        FROM
                                                                                                                fl
                                                                                                )
                                                                                                SELECT
                                                                                                        company_id,
                                                                                                        end_date AS earliest_date,
                                                                                                        LISTAGG(
                                                                                                                contract_number,
                                                                                                                ','
                                                                                                        ) AS contract_number,
                                                                                                        LISTAGG(
                                                                                                                product_categorization_arr_reported_product,
                                                                                                                ','
                                                                                                        ) AS products
                                                                                                FROM
                                                                                                        sl
                                                                                                WHERE
                                                                                                        daysfilterflag = 1
                                                                                                GROUP BY
                                                                                                        1,
                                                                                                        2
                                                                                        ),
                                                                                        customer_healthscores AS (
                                                                                                SELECT
                                                                                                        ship_to AS company_id,
                                                                                                        healthscore,
                                                                                                        healthscore_alpha
                                                                                                FROM
                                                                                                        analytics.dbo_transformation.partner_success_intermediate__healthscores qualify ROW_NUMBER() over (
                                                                                                                PARTITION BY ship_to
                                                                                                                ORDER BY
                                                                                                                        apdate DESC
                                                                                                        ) = 1
                                                                                        ),
                                                                                        customer_touch_tier AS (
                                                                                                SELECT
                                                                                                        applied_date,
                                                                                                        ship_to,
                                                                                                        COUNT(
                                                                                                                DISTINCT touch_tier
                                                                                                        ) AS tt_count,
                                                                                                        LISTAGG(
                                                                                                                DISTINCT touch_tier,
                                                                                                                ' | '
                                                                                                        ) within GROUP (
                                                                                                                ORDER BY
                                                                                                                        touch_tier
                                                                                                        ) AS tt_classes,
                                                                                                        MIN(touch_tier) AS tt_min,
                                                                                                        MAX(touch_tier) AS tt_max,
                                                                                                        MIN(touch_tier) AS touch_tier
                                                                                                FROM
                                                                                                        analytics.dbo_transformation.partner_success_intermediate__tech_touch_roster
                                                                                                GROUP BY
                                                                                                        1,
                                                                                                        2
                                                                                        ),
                                                                                        customer_contract_type AS (
                                                                                                SELECT
                                                                                                        applied_date,
                                                                                                        ship_to,
                                                                                                        LISTAGG(
                                                                                                                DISTINCT COALESCE(
                                                                                                                        quote_line_subscription_type,
                                                                                                                        'Non-Bedrock M2M'
                                                                                                                ),
                                                                                                                ' | '
                                                                                                        ) within GROUP (
                                                                                                                ORDER BY
                                                                                                                        COALESCE(
                                                                                                                                quote_line_subscription_type,
                                                                                                                                'Non-Bedrock M2M'
                                                                                                                        )
                                                                                                        ) AS contract_type
                                                                                                FROM
                                                                                                        analytics.dbo.core__rpt_billings
                                                                                                GROUP BY
                                                                                                        1,
                                                                                                        2
                                                                                        ),
                                                                                        customer_psa_package AS (
                                                                                                SELECT
                                                                                                        company_id,-- company_name_with_id,
                                                                                                        LISTAGG(
                                                                                                                DISTINCT IFF(
                                                                                                                        product_categorization_product_package = 'Manage'
                                                                                                                        AND product_categorization_product_plan IN (
                                                                                                                                'Basic',
                                                                                                                                'Standard',
                                                                                                                                'Premium'
                                                                                                                        ),
                                                                                                                        product_categorization_product_plan,
                                                                                                                        'Legacy'
                                                                                                                ),
                                                                                                                ' | '
                                                                                                        ) within GROUP (
                                                                                                                ORDER BY
                                                                                                                        IFF(
                                                                                                                                product_categorization_product_package = 'Manage'
                                                                                                                                AND product_categorization_product_plan IN (
                                                                                                                                        'Basic',
                                                                                                                                        'Standard',
                                                                                                                                        'Premium'
                                                                                                                                ),
                                                                                                                                product_categorization_product_plan,
                                                                                                                                'Legacy'
                                                                                                                        )
                                                                                                        ) AS psa_package
                                                                                                FROM
                                                                                                        analytics.dbo.growth__obt
                                                                                                WHERE
                                                                                                        reporting_date = (
                                                                                                                SELECT
                                                                                                                        DISTINCT CASE
                                                                                                                                WHEN DAY(CURRENT_DATE()) > 2 THEN DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -1))
                                                                                                                                ELSE DATE_TRUNC('Month', ADD_MONTHS(CURRENT_DATE() :: DATE, -2))END AS DATE
                                                                                                                        )
                                                                                                                        AND metric_object = 'applied_billings'
                                                                                                                        AND product_categorization_product_line = 'Manage'
                                                                                                                        AND billings > 0
                                                                                                                GROUP BY
                                                                                                                        1
                                                                                                        ),
                                                                                                        current_monthly AS (
                                                                                                                SELECT
                                                                                                                        arr.company_id,
                                                                                                                        product,
                                                                                                                        "Seat Type",
                                                                                                                        SUM(billingslocalunified) AS "Current Monthly Total",
                                                                                                                        CASE
                                                                                                                                WHEN product IN (
                                                                                                                                        'Manage',
                                                                                                                                        'Sell',
                                                                                                                                        'BrightGauge',
                                                                                                                                        'ItBoost'
                                                                                                                                )
                                                                                                                                AND (
                                                                                                                                        "Seat Type" = 'Include in ARR calculation'
                                                                                                                                        OR "Seat Type" IS NULL
                                                                                                                                ) THEN SUM(billingslocalunified)
                                                                                                                        END AS cmp
                                                                                                                FROM
                                                                                                                        dataiku.dev_dataiku_staging.pnp_dashboard_arr_and_billing_c arr
                                                                                                                        LEFT JOIN (
                                                                                                                                SELECT
                                                                                                                                        company_id,
                                                                                                                                        company_name_id
                                                                                                                                FROM
                                                                                                                                        dataiku.dev_dataiku_staging.pnp_company_dim
                                                                                                                        ) C
                                                                                                                        ON C.company_id = arr.company_id
                                                                                                                WHERE
                                                                                                                        reporting_date = (
                                                                                                                                SELECT
                                                                                                                                        MAX(reporting_date)
                                                                                                                                FROM
                                                                                                                                        dataiku.dev_dataiku_staging.pnp_dashboard_arr_and_billing_c
                                                                                                                        )
                                                                                                                        AND product IN (
                                                                                                                                'Manage',
                                                                                                                                'Sell',
                                                                                                                                'BrightGauge'
                                                                                                                        )
                                                                                                                GROUP BY
                                                                                                                        1,
                                                                                                                        2,
                                                                                                                        3
                                                                                                        ),
                                                                                                        monthly_price_cmp AS (
                                                                                                                SELECT
                                                                                                                        company_id,
                                                                                                                        SUM("Current Monthly Total") AS "Current Monthly Total",
                                                                                                                        SUM(cmp) AS cmp
                                                                                                                FROM
                                                                                                                        current_monthly
                                                                                                                GROUP BY
                                                                                                                        1
                                                                                                        ),
                                                                                                        current_monthly_rmm AS (
                                                                                                                SELECT
                                                                                                                        arr.company_id,
                                                                                                                        product,
                                                                                                                        "Seat Type",
                                                                                                                        SUM(billingslocalunified) AS "Current Monthly Total RMM",
                                                                                                                        CASE
                                                                                                                                WHEN product IN (
                                                                                                                                        'Automate',
                                                                                                                                        'Command',
                                                                                                                                        'CW RMM'
                                                                                                                                )
                                                                                                                                AND (
                                                                                                                                        "Seat Type" = 'Include in ARR calculation'
                                                                                                                                        OR "Seat Type" IS NULL
                                                                                                                                ) THEN SUM(billingslocalunified)
                                                                                                                        END AS cmp_rmm
                                                                                                                FROM
                                                                                                                        dataiku.dev_dataiku_staging.pnp_dashboard_arr_and_billing_c arr
                                                                                                                        LEFT JOIN (
                                                                                                                                SELECT
                                                                                                                                        company_id,
                                                                                                                                        company_name_id
                                                                                                                                FROM
                                                                                                                                        dataiku.dev_dataiku_staging.pnp_company_dim
                                                                                                                        ) C
                                                                                                                        ON C.company_id = arr.company_id
                                                                                                                WHERE
                                                                                                                        reporting_date = (
                                                                                                                                SELECT
                                                                                                                                        MAX(reporting_date)
                                                                                                                                FROM
                                                                                                                                        dataiku.dev_dataiku_staging.pnp_dashboard_arr_and_billing_c
                                                                                                                        )
                                                                                                                        AND product IN (
                                                                                                                                'Automate',
                                                                                                                                'Command',
                                                                                                                                'CW RMM'
                                                                                                                        )
                                                                                                                GROUP BY
                                                                                                                        1,
                                                                                                                        2,
                                                                                                                        3
                                                                                                        ),
                                                                                                        monthly_price_cmp_rmm AS (
                                                                                                                SELECT
                                                                                                                        company_id,
                                                                                                                        SUM("Current Monthly Total RMM") AS "Current Monthly Total RMM",
                                                                                                                        SUM(cmp_rmm) AS cmp_rmm
                                                                                                                FROM
                                                                                                                        current_monthly_rmm
                                                                                                                GROUP BY
                                                                                                                        1
                                                                                                        ),
                                                                                                        customer_tenure AS (
                                                                                                                SELECT
                                                                                                                        company_id,
                                                                                                                        MIN(corporate_billing_start_date) AS corporate_start_date,
                                                                                                                        MAX(corporate_billing_reporting_period) AS corporate_tenure,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_group = 'Client & Process Mgmt',
                                                                                                                                        product_group_billing_start_date,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS psa_start_date,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_group = 'Client & Process Mgmt',
                                                                                                                                        product_group_billing_reporting_period,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS psa_tenure,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_group = 'Remote Monitoring & Mgmt',
                                                                                                                                        product_group_billing_start_date,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS rmm_start_date,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_group = 'Remote Monitoring & Mgmt',
                                                                                                                                        product_group_billing_reporting_period,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS rmm_tenure,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_portfolio = 'Security Mgmt',
                                                                                                                                        product_portfolio_billing_start_date,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS security_start_date,
                                                                                                                        MAX(
                                                                                                                                IFF(
                                                                                                                                        product_categorization_product_portfolio = 'Security Mgmt',
                                                                                                                                        product_portfolio_billing_reporting_period,
                                                                                                                                        NULL
                                                                                                                                )
                                                                                                                        ) AS security_tenure
                                                                                                                FROM
                                                                                                                        analytics.dbo.growth__obt
                                                                                                                GROUP BY
                                                                                                                        1
                                                                                                        )
                                                                                                SELECT
                                                                                                        DISTINCT -- removed duplicates -- customer id cr.company_id,
                                                                                                        cr.company_name,
                                                                                                        CONCAT(
                                                                                                                cr.company_id,
                                                                                                                cr.company_name
                                                                                                        ) AS company_name_with_id,-- CURRENT arr cr.current_arr,
                                                                                                        COALESCE(
                                                                                                                ctt.touch_tier,
                                                                                                                'Tech Touch (due to non-qualifying MRR)'
                                                                                                        ) AS touch_tier,-- contract TYPE cct.contract_type,
                                                                                                        cr.itnation_peer_group_active_partner,
                                                                                                        CAST(
                                                                                                                chs.healthscore AS STRING
                                                                                                        ) AS healthscore,-- converted healthscore TO STRING chs.healthscore_alpha,
                                                                                                        ct.corporate_start_date,
                                                                                                        ct.corporate_tenure,
                                                                                                        ct.psa_start_date,
                                                                                                        ct.psa_tenure,
                                                                                                        cpp.psa_package,
                                                                                                        ct.rmm_start_date,
                                                                                                        ct.rmm_tenure,
                                                                                                        ct.security_start_date,
                                                                                                        ct.security_tenure,
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
                                                                                                        cr.automate_units_calc,
                                                                                                        cr.command_desktop_units,
                                                                                                        cr.command_network_units,
                                                                                                        cr.command_server_units,
                                                                                                        cr.help_desk_units,
                                                                                                        cr.security_units,
                                                                                                        (
                                                                                                                cr.sell_arr
                                                                                                        ) AS sell_arr,
                                                                                                        (
                                                                                                                cr.brightgauge_arr
                                                                                                        ) AS brightgauge_arr,
                                                                                                        (
                                                                                                                cr.itboost_arr
                                                                                                        ) AS itboost_arr,
                                                                                                        (
                                                                                                                cr.rmm_additional_arr
                                                                                                        ) AS rmm_additional_arr,
                                                                                                        (
                                                                                                                cr.help_desk_arr
                                                                                                        ) AS help_desk_arr,
                                                                                                        (
                                                                                                                cr.security_arr
                                                                                                        ) AS security_arr,
                                                                                                        (
                                                                                                                cr.other_arr
                                                                                                        ) AS other_arr,
                                                                                                        (
                                                                                                                cr.command_arr
                                                                                                        ) AS command_arr,
                                                                                                        cr.sell_cloud,
                                                                                                        cr.sell_legacy_on_prem,
                                                                                                        cr.sell_on_prem,
                                                                                                        cr.brightgauge_cloud,
                                                                                                        cr.brightgauge_legacy_on_prem,
                                                                                                        cr.brightgauge_on_prem,
                                                                                                        cr.itboost_cloud,
                                                                                                        cr.itboost_legacy_on_prem,
                                                                                                        cr.itboost_on_prem,
                                                                                                        cr.help_desk_cloud,
                                                                                                        cr.help_desk_legacy_on_prem,
                                                                                                        cr.help_desk_on_prem,
                                                                                                        cr.security_cloud,
                                                                                                        cr.security_legacy_on_prem,
                                                                                                        cr.security_on_prem,
                                                                                                        cr.other_cloud,
                                                                                                        cr.other_legacy_on_prem,
                                                                                                        cr.other_on_prem,
                                                                                                        cr.third_party_mrr,
                                                                                                        cr.webroot_mrr,
                                                                                                        cr.webroot_units,
                                                                                                        cr.webroot_overage_mrr,
                                                                                                        cr.webroot_overage_units,
                                                                                                        C.contract_number,
                                                                                                        C.earliest_date,
                                                                                                        C.products,-- START OF expanded COLUMNS
                                                                                                        AND removed amc.company_id,
                                                                                                        manage category,
                                                                                                        AND automate category IFF(
                                                                                                                psa_legacy_on_prem IS NULL,
                                                                                                                0,
                                                                                                                psa_legacy_on_prem
                                                                                                        ) AS psa_legacy_on_prem,
                                                                                                        IFF(
                                                                                                                psa_on_prem IS NULL,
                                                                                                                0,
                                                                                                                psa_on_prem
                                                                                                        ) AS psa_on_prem,
                                                                                                        IFF(
                                                                                                                psa_cloud IS NULL,
                                                                                                                0,
                                                                                                                psa_cloud
                                                                                                        ) AS psa_cloud,
                                                                                                        automate_legacy_on_prem AS rmm_legacy_on_prem,-- changed NAME TO rmm legacy
                                                                                                        ON prem automate_legacy_on_prem,
                                                                                                        automate_on_prem AS rmm_on_prem,-- changed NAME TO rmm
                                                                                                        ON prem,
                                                                                                        automate_on_prem,
                                                                                                        automate_cloud AS rmm_cloud,-- changed NAME TO rmm cloud automate_cloud,
                                                                                                        IFF(
                                                                                                                psa_arr IS NULL,
                                                                                                                0,
                                                                                                                psa_arr
                                                                                                        ) AS psa_arr,
                                                                                                        automate_arr,
                                                                                                        IFF(
                                                                                                                psa_units IS NULL,
                                                                                                                0,
                                                                                                                psa_units
                                                                                                        ) AS psa_units,
                                                                                                        automate_units,
                                                                                                        IFF(
                                                                                                                psa_on_prem_arr IS NULL,
                                                                                                                0,
                                                                                                                psa_on_prem_arr
                                                                                                        ) AS psa_on_prem_arr,
                                                                                                        IFF(
                                                                                                                psa_cloud_arr IS NULL,
                                                                                                                0,
                                                                                                                psa_cloud_arr
                                                                                                        ) AS psa_cloud_arr,
                                                                                                        IFF(
                                                                                                                psa_legacy_on_prem_arr IS NULL,
                                                                                                                0,
                                                                                                                psa_legacy_on_prem_arr
                                                                                                        ) AS psa_legacy_on_prem_arr,
                                                                                                        CAST(
                                                                                                                IFF(
                                                                                                                        psa_on_prem_units IS NULL,
                                                                                                                        0,
                                                                                                                        psa_on_prem_units
                                                                                                                ) AS INT
                                                                                                        ) AS psa_on_prem_units,
                                                                                                        IFF(
                                                                                                                psa_cloud_units IS NULL,
                                                                                                                0,
                                                                                                                psa_cloud_units
                                                                                                        ) AS psa_cloud_units,
                                                                                                        CAST(
                                                                                                                IFF(
                                                                                                                        psa_legacy_on_prem_units IS NULL,
                                                                                                                        0,
                                                                                                                        psa_legacy_on_prem_units
                                                                                                                ) AS INT
                                                                                                        ) AS psa_legacy_on_prem_units,
                                                                                                        IFF(
                                                                                                                automate_cloud_arr IS NULL,
                                                                                                                0,
                                                                                                                automate_cloud_arr
                                                                                                        ) AS automate_cloud_arr,
                                                                                                        IFF(
                                                                                                                automate_on_prem_arr IS NULL,
                                                                                                                0,
                                                                                                                automate_on_prem_arr
                                                                                                        ) AS automate_on_prem_arr,
                                                                                                        IFF(
                                                                                                                automate_legacy_on_prem_arr IS NULL,
                                                                                                                0,
                                                                                                                automate_legacy_on_prem_arr
                                                                                                        ) AS automate_legacy_on_prem_arr,
                                                                                                        IFF(
                                                                                                                automate_cloud_units IS NULL,
                                                                                                                0,
                                                                                                                automate_cloud_units
                                                                                                        ) AS automate_cloud_units,
                                                                                                        IFF(
                                                                                                                automate_on_prem_units IS NULL,
                                                                                                                0,
                                                                                                                automate_on_prem_units
                                                                                                        ) AS automate_on_prem_units,
                                                                                                        CAST(
                                                                                                                IFF(
                                                                                                                        automate_legacy_on_prem_units IS NULL,
                                                                                                                        0,
                                                                                                                        automate_legacy_on_prem_units
                                                                                                                ) AS INT
                                                                                                        ) AS automate_legacy_on_prem_units,
                                                                                                        CASE
                                                                                                                -- hybrid flag
                                                                                                                WHEN psa_on_prem = 1 THEN 1
                                                                                                                WHEN sell_on_prem = 1 THEN 1
                                                                                                                WHEN brightgauge_on_prem = 1 THEN 1
                                                                                                                WHEN itboost_on_prem = 1 THEN 1
                                                                                                                WHEN rmm_on_prem = 1 THEN 1
                                                                                                                WHEN security_on_prem = 1 THEN 1
                                                                                                                WHEN other_on_prem = 1 THEN 1
                                                                                                                ELSE 0
                                                                                                        END AS hybrid_flag,
                                                                                                        CASE
                                                                                                                --
                                                                                                                ON prem flag
                                                                                                                WHEN psa_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN sell_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN brightgauge_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN itboost_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN help_desk_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN security_legacy_on_prem = 1 THEN 1
                                                                                                                WHEN other_legacy_on_prem = 1 THEN 1
                                                                                                                ELSE 0
                                                                                                        END AS "On-Prem Flag",
                                                                                                        (
                                                                                                                hybrid_flag + "On-Prem Flag"
                                                                                                        ) AS "On-Prem/Hybrid",
                                                                                                        CASE
                                                                                                                WHEN healthscore_alpha = 'A' THEN 'A-B'
                                                                                                                WHEN healthscore_alpha = 'B' THEN 'A-B'
                                                                                                                WHEN healthscore_alpha = 'C' THEN 'C'
                                                                                                                WHEN healthscore_alpha = 'D' THEN 'D-F'
                                                                                                                WHEN healthscore_alpha = 'F' THEN 'F'
                                                                                                                ELSE 'None'
                                                                                                        END AS "Gainsight Risk",
                                                                                                        IFF(
                                                                                                                itnation_peer_group_active_partner = 1,
                                                                                                                'Active Member',
                                                                                                                'No'
                                                                                                        ) AS "IT Nation",
                                                                                                        CASE
                                                                                                                -- rmm PACKAGE
                                                                                                                WHEN automate_active_partner > 0
                                                                                                                AND webroot_active_partner > 0
                                                                                                                AND brightgauge_active_partner = 0 THEN 'CW-RMM-EPB-STANDARD'
                                                                                                                WHEN automate_active_partner > 0
                                                                                                                AND webroot_active_partner > 0
                                                                                                                AND brightgauge_active_partner > 0 THEN 'CW-RMM--ADVANCED-EPP'
                                                                                                                WHEN automate_active_partner > 0
                                                                                                                AND webroot_active_partner = 0
                                                                                                                AND brightgauge_active_partner = 0 THEN 'CWRMMEPBSTND-W-O-EPP'
                                                                                                                WHEN automate_active_partner > 0
                                                                                                                AND webroot_active_partner = 0
                                                                                                                AND brightgauge_active_partner > 0 THEN 'CW-RMM-ADV-WOUT-EPP'
                                                                                                                ELSE 'None'
                                                                                                        END AS "Rmm Package",
                                                                                                        CASE
                                                                                                                WHEN touch_tier <> '0' THEN touch_tier
                                                                                                                WHEN touch_tier = '0' THEN 'None'
                                                                                                                ELSE NULL
                                                                                                        END AS "Deal Value",
                                                                                                        IFF(
                                                                                                                healthscore_alpha = '0',
                                                                                                                1,
                                                                                                                0
                                                                                                        ) AS "Gainsight Score Available",
                                                                                                        CONCAT(
                                                                                                                healthscore_alpha,
                                                                                                                '-',
                                                                                                                healthscore
                                                                                                        ) AS "Health Score Grade",
                                                                                                        IFF(
                                                                                                                command_active_partner = 1,
                                                                                                                (
                                                                                                                        command_desktop_units + command_server_units + help_desk_units
                                                                                                                ),
                                                                                                                0
                                                                                                        ) AS rmm_units_additive,
                                                                                                        (
                                                                                                                command_desktop_units + command_network_units + command_server_units + automate_units
                                                                                                        ) AS rmm_units,
                                                                                                        (
                                                                                                                automate_arr + cr.command_arr
                                                                                                        ) AS rmm_arr,
                                                                                                        psa_package,
                                                                                                        CASE
                                                                                                                WHEN psa_package = 'Premium' THEN 'Best'
                                                                                                                WHEN psa_package = 'Legacy | Premium' THEN 'Best'
                                                                                                                WHEN psa_package = 'Premium | Standard' THEN 'Best'
                                                                                                                WHEN psa_package = 'Basic | Legacy | Standard' THEN 'Best'
                                                                                                                WHEN psa_package = 'Legacy' THEN 'Better'
                                                                                                                WHEN psa_package = 'Standard' THEN 'Better'
                                                                                                                WHEN psa_package = 'Legacy | Standard' THEN 'Better'
                                                                                                                WHEN psa_package = 'Basic' THEN 'Good'
                                                                                                                WHEN psa_package = 'Basic | Legacy' THEN 'Good'
                                                                                                                WHEN psa_package = 'Basic | Standard' THEN 'Good'
                                                                                                                WHEN psa_package IS NULL THEN NULL
                                                                                                        END AS legacy,
                                                                                                        CASE
                                                                                                                WHEN sell_active_partner > 0 THEN 'Best'
                                                                                                                WHEN brightgauge_active_partner = 0
                                                                                                                AND legacy IS NOT NULL THEN legacy
                                                                                                                WHEN manage_active_partner > 0
                                                                                                                AND brightgauge_active_partner > 0 THEN 'Better'
                                                                                                                ELSE 'None'
                                                                                                        END AS "PSA Package Active Use FINAL",
                                                                                                        CASE
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Better' THEN 'Bus Mgmt Standard'
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Best' THEN 'Bus Mgmt Advanced'
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Good' THEN 'Bus Mgmt Core'
                                                                                                                ELSE NULL
                                                                                                        END AS future,
                                                                                                        CASE
                                                                                                                WHEN future = 'Bus Mgmt Advanced' THEN MAX(
                                                                                                                        pb."Best"
                                                                                                                )
                                                                                                                WHEN future = 'Bus Mgmt Standard' THEN MAX(
                                                                                                                        pb."Better"
                                                                                                                )
                                                                                                                WHEN future = 'Bus Mgmt Core' THEN MAX(
                                                                                                                        pb."Good"
                                                                                                                )
                                                                                                                ELSE NULL
                                                                                                        END AS "List Price",
                                                                                                        MAX(
                                                                                                                pb.lowerbound
                                                                                                        ) AS max_lowerbound,
                                                                                                        MAX(
                                                                                                                rmmpb.lowerbound
                                                                                                        ) AS max_lowerbound_rmm,
                                                                                                        CASE
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Better' THEN MIN(
                                                                                                                        pb."Better"
                                                                                                                )
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Best' THEN MIN(
                                                                                                                        pb."Best"
                                                                                                                )
                                                                                                                WHEN "PSA Package Active Use FINAL" = 'Good' THEN MIN(
                                                                                                                        pb."Good"
                                                                                                                )
                                                                                                                ELSE 0
                                                                                                        END AS "Bus Mgmt Future Price Per Seat",
                                                                                                        (
                                                                                                                psa_units * "Bus Mgmt Future Price Per Seat"
                                                                                                        ) AS "Future Monthly Price",
                                                                                                        "Current Monthly Total",
                                                                                                        cmp,
                                                                                                        (
                                                                                                                "Future Monthly Price" - cmp
                                                                                                        ) / nullifzero(cmp) "Monthly Price Increase %",
                                                                                                        MAX(reference_currency) AS reference_currency,
                                                                                                        IFF(
                                                                                                                automate_active_partner > 0,
                                                                                                                'Essentials WO RPP',
                                                                                                                IFF(
                                                                                                                        command_active_partner > 0,
                                                                                                                        'Pro W EPP',
                                                                                                                        'Undefined'
                                                                                                                )
                                                                                                        ) AS future_RMM,
                                                                                                        CASE
                                                                                                                WHEN future_RMM = 'Pro W EPP' THEN CAST(MIN(rmmpb."CW-RMM-ADV-WOUT-EPP") AS FLOAT)
                                                                                                                WHEN future_RMM = 'Essentials WO RPP' THEN CAST(MIN(rmmpb."CW-RMM-EPB-STANDARD") AS FLOAT)
                                                                                                                WHEN future_RMM = 'Undefined' THEN CAST(MIN(rmmpb."CW-RMM--ADVANCED-EPP") AS FLOAT)
                                                                                                        END AS price_per_seat_rmm,
                                                                                                        CASE
                                                                                                                WHEN future_RMM = 'Pro W EPP' THEN CAST(MAX(rmmpb."CW-RMM-ADV-WOUT-EPP") AS FLOAT)
                                                                                                                WHEN future_RMM = 'Essentials WO RPP' THEN CAST(MAX(rmmpb."CW-RMM-EPB-STANDARD") AS FLOAT)
                                                                                                                WHEN future_RMM = 'Undefined' THEN CAST(MAX(rmmpb."CW-RMM--ADVANCED-EPP") AS FLOAT)
                                                                                                        END AS list_price_rmm,
                                                                                                        (
                                                                                                                rmm_units * price_per_seat_rmm
                                                                                                        ) AS "Future Monthly Price RMM",
                                                                                                        "Current Monthly Total RMM",
                                                                                                        cmp_rmm,
                                                                                                        (
                                                                                                                "Future Monthly Price RMM" - cmp_rmm
                                                                                                        ) / nullifzero(cmp_rmm) "Monthly Price Increase RMM %",
                                                                                                        CASE
                                                                                                                WHEN (
                                                                                                                        rmm_units + rmm_units_additive + automate_legacy_on_prem_units
                                                                                                                ) = 0 THEN NULL
                                                                                                                WHEN (
                                                                                                                        command_active_partner = 1
                                                                                                                        AND automate_active_partner = 1
                                                                                                                ) THEN rmm_units
                                                                                                                WHEN (
                                                                                                                        command_active_partner = 1
                                                                                                                        AND automate_active_partner = 0
                                                                                                                ) THEN rmm_units_additive
                                                                                                                WHEN (
                                                                                                                        command_active_partner = 0
                                                                                                                        AND automate_active_partner = 1
                                                                                                                ) THEN rmm_units
                                                                                                                ELSE 0
                                                                                                        END AS "Total RMM Units"
                                                                                                FROM
                                                                                                        customer_roster cr
                                                                                                        LEFT JOIN contract C
                                                                                                        ON C.company_id = cr.company_id
                                                                                                        LEFT JOIN customer_healthscores chs
                                                                                                        ON chs.company_id = cr.company_id
                                                                                                        LEFT JOIN customer_touch_tier ctt
                                                                                                        ON ctt.applied_date = cr.reporting_date
                                                                                                        AND ctt.ship_to = cr.company_id
                                                                                                        LEFT JOIN customer_contract_type cct
                                                                                                        ON cct.applied_date = cr.reporting_date
                                                                                                        AND cct.ship_to = cr.company_id
                                                                                                        LEFT JOIN customer_psa_package cpp
                                                                                                        ON cpp.company_id = cr.company_id
                                                                                                        LEFT JOIN customer_tenure ct
                                                                                                        ON ct.company_id = cr.company_id
                                                                                                        LEFT JOIN dev_dataiku_staging.pnp_dashboard_automate_and_manage_calc_c amc
                                                                                                        ON amc.company_id = cr.company_id -- merged queries
                                                                                                        LEFT JOIN dataiku.dev_dataiku_staging.pnp_dashboard_arr_and_billing_c arr_c
                                                                                                        ON arr_c.company_id = cr.company_id
                                                                                                        LEFT JOIN dataiku.dev_dataiku_staging.pnp_dashboard_business_management_pricebook_staging pb
                                                                                                        ON pb.cur = reference_currency
                                                                                                        AND pb.lowerbound <= psa_units
                                                                                                        LEFT JOIN dataiku.dev_dataiku_staging.pnp_dashboard_rmm_pricebook_staging rmmpb
                                                                                                        ON rmmpb.cur = reference_currency
                                                                                                        AND rmmpb.lowerbound <= (
                                                                                                                command_server_units + command_network_units + command_desktop_units + automate_units
                                                                                                        )
                                                                                                        LEFT JOIN monthly_price_cmp
                                                                                                        ON cr.company_id = monthly_price_cmp.company_id
                                                                                                        LEFT JOIN monthly_price_cmp_rmm
                                                                                                        ON cr.company_id = monthly_price_cmp_rmm.company_id
                                                                                                WHERE
                                                                                                        current_arr <> 0 -- filtered CURRENT arr TO NOT be 0 --
                                                                                                        AND cr.company_id = '0016g00000pUlrcAAC'
                                                                                                        AND cr.company_id NOT IN (
                                                                                                                'lopez@cinformatique.ch',
                                                                                                                'JEREMY.A.BECKER@GMAIL.COM',
                                                                                                                'blairphillips@gmail.com',
                                                                                                                'Chad@4bowers.net',
                                                                                                                'dev@bcsint.com',
                                                                                                                'bob@compu-gen.com',
                                                                                                                'Greg@ablenetworksnj.com',
                                                                                                                'screenconnect.com@solutionssquad.com',
                                                                                                                'andrew@gmal.co.uk'
                                                                                                        ) -- filtered rows TO exclude these
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
                                                                                                        psa_units,
                                                                                                        "Current Monthly Total",
                                                                                                        cmp,
                                                                                                        future,
                                                                                                        future_RMM,
                                                                                                        "Current Monthly Total RMM",
                                                                                                        cmp_rmm,
                                                                                                        automate_legacy_on_prem_units
