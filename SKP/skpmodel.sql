WITH customer_roster AS (
    SELECT
        reporting_date,
        company_name_with_id,
        company_name,
        company_id,
        MAX(
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
        SUM(
            IFF(
                product_categorization_product_line = 'Manage'
                AND product_categorization_product_package = 'Manage'
                AND product_categorization_product_plan IN (
                    '-',
                    'Standard',
                    'Premium',
                    'Basic'
                ),
                units,
                0
            )
        ) AS psa_units,
        SUM(
            IFF(
                product_categorization_product_line IN (
                    'Automate',
                    'Command'
                )
                AND product_categorization_product_package IN (
                    'Automate',
                    'Desktops',
                    'Networks',
                    'Servers'
                ),
                units,
                0
            )
        ) AS rmm_units,
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
                    ROUND(SUM(billings)) AS current_billings,
                    ROUND(SUM(arr)) AS current_arr,
                    SUM(
                        IFF(
                            product_categorization_product_line = 'Manage'
                            AND product_categorization_product_package = 'Manage'
                            AND product_categorization_product_plan IN (
                                '-',
                                'Standard',
                                'Premium',
                                'Basic'
                            ),
                            arr,
                            0
                        )
                    ) AS psa_arr,
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
                        IFF(
                            product_categorization_product_line IN (
                                'Automate',
                                'Command'
                            )
                            AND product_categorization_product_package IN (
                                'Automate',
                                'Desktops',
                                'Networks',
                                'Servers'
                            ),
                            arr,
                            0
                        )
                    ) AS rmm_arr,
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
                            product_categorization_product_line = 'Manage'
                            AND product_categorization_product_package = 'Manage'
                            AND product_categorization_product_plan IN (
                                '-',
                                'Standard',
                                'Premium',
                                'Basic'
                            )
                            AND product_categorization_license_service_type = 'SaaS',
                            1,
                            0
                        )
                    ) AS psa_cloud,
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
                    ) AS itboost_cloud,
                    MAX(
                        IFF(
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
                            AND product_categorization_license_service_type = 'SaaS',
                            1,
                            0
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
                            product_categorization_product_line = 'Manage'
                            AND product_categorization_product_package = 'Manage'
                            AND product_categorization_product_plan IN (
                                '-',
                                'Standard',
                                'Premium',
                                'Basic'
                            )
                            AND product_categorization_license_service_type IN (
                                'Perpetual',
                                'Maintenance'
                            ),
                            1,
                            0
                        )
                    ) AS psa_legacy_on_prem,
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
                            AND product_categorization_license_service_type IN (
                                'Perpetual',
                                'Maintenance'
                            ),
                            1,
                            0
                        )
                    ) AS rmm_legacy_on_prem,
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
                            product_categorization_product_line = 'Manage'
                            AND product_categorization_product_package = 'Manage'
                            AND product_categorization_product_plan IN (
                                '-',
                                'Standard',
                                'Premium',
                                'Basic'
                            )
                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                            1,
                            0
                        )
                    ) AS psa_on_prem,
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
                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                            1,
                            0
                        )
                    ) AS rmm_on_prem,
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
                                    AND item_id = '3P-SAAS3002315EPPRMM', units, 0)) AS webroot_overage_units,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type = 'SaaS',
                                            arr,
                                            0
                                        )
                                    ) AS psa_cloud_arr,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                            arr,
                                            0
                                        )
                                    ) AS psa_on_prem_arr,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type IN (
                                                'Perpetual',
                                                'Maintenance'
                                            ),
                                            arr,
                                            0
                                        )
                                    ) AS psa_legacy_on_prem_arr,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type = 'SaaS',
                                            arr,
                                            0
                                        )
                                    ) AS automate_cloud_arr,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                            arr,
                                            0
                                        )
                                    ) AS automate_on_prem_arr,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type IN (
                                                'Perpetual',
                                                'Maintenance'
                                            ),
                                            arr,
                                            0
                                        )
                                    ) AS automate_legacy_on_prem_arr,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Control'
                                            AND product_categorization_license_service_type = 'SaaS',
                                            arr,
                                            0
                                        )
                                    ) AS control_cloud_arr,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Control'
                                            AND product_categorization_license_service_type IN (
                                                'Perpetual',
                                                'Maintenance'
                                            ),
                                            arr,
                                            0
                                        )
                                    ) AS control_on_prem_arr,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type = 'SaaS',
                                            units,
                                            0
                                        )
                                    ) AS psa_cloud_units,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                            units,
                                            0
                                        )
                                    ) AS psa_on_prem_units,
                                    SUM(
                                        IFF(
                                            product_categorization_product_line = 'Manage'
                                            AND product_categorization_product_package = 'Manage'
                                            AND product_categorization_product_plan IN (
                                                '-',
                                                'Standard',
                                                'Premium',
                                                'Basic'
                                            )
                                            AND product_categorization_license_service_type IN (
                                                'Perpetual',
                                                'Maintenance'
                                            ),
                                            units,
                                            0
                                        )
                                    ) AS psa_legacy_on_prem_units,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type = 'SaaS',
                                            units,
                                            0
                                        )
                                    ) AS automate_cloud_units,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type IN ('On Premise (Subscription)'),
                                            units,
                                            0
                                        )
                                    ) AS automate_on_prem_units,
                                    SUM(
                                        IFF(
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
                                            AND product_categorization_license_service_type IN (
                                                'Perpetual',
                                                'Maintenance'
                                            ),
                                            units,
                                            0
                                        )
                                    ) AS automate_legacy_on_prem_units
                                    FROM
                                        analytics.dbt_baw.growth__obt
                                    WHERE
                                        reporting_date = '2021-10-01' -- DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE))
                                        AND metric_object = 'applied_billings'
                                    GROUP BY
                                        1,
                                        2,
                                        3,
                                        4
                                    HAVING
                                        SUM(billings) > 0
                                ),
                                customer_2020_stats AS (
                                    SELECT
                                        company_name_with_id,
                                        ROUND(SUM(billings)) AS dec_2020_billings,
                                        ROUND(SUM(arr)) AS dec_2020_arr
                                    FROM
                                        analytics.dbt_baw.growth__obt
                                    WHERE
                                        reporting_date = '2020-12-01'
                                        AND metric_object = 'applied_billings'
                                    GROUP BY
                                        1
                                    HAVING
                                        SUM(billings) > 0
                                ),
                                customer_2019_stats AS (
                                    SELECT
                                        company_name_with_id,
                                        ROUND(SUM(billings)) AS dec_2019_billings,
                                        ROUND(SUM(arr)) AS dec_2019_arr
                                    FROM
                                        analytics.dbt_baw.growth__obt
                                    WHERE
                                        reporting_date = '2019-12-01'
                                        AND metric_object = 'applied_billings'
                                    GROUP BY
                                        1
                                    HAVING
                                        SUM(billings) > 0
                                ),
                                customer_2018_stats AS (
                                    SELECT
                                        company_name_with_id,
                                        ROUND(SUM(billings)) AS dec_2018_billings,
                                        ROUND(SUM(arr)) AS dec_2018_arr
                                    FROM
                                        analytics.dbt_baw.growth__obt
                                    WHERE
                                        reporting_date = '2018-12-01'
                                        AND metric_object = 'applied_billings'
                                    GROUP BY
                                        1
                                    HAVING
                                        SUM(billings) > 0
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
                                        company_id,
                                        company_name_with_id,
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
                                        analytics.dbt_baw.growth__obt
                                    WHERE
                                        reporting_date = '2021-10-01' -- DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE))
                                        AND metric_object = 'applied_billings'
                                        AND product_categorization_product_line = 'Manage'
                                        AND billings > 0
                                    GROUP BY
                                        1,
                                        2
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
                                        analytics.dbt_baw.growth__obt
                                    GROUP BY
                                        1
                                )
                                SELECT
                                    -- customer id cr.company_id,
                                    cr.company_name,
                                    cr.company_name_with_id,-- CURRENT arr cr.current_arr,- - 2020 arr (
                                        if available
                                    ) c20.dec_2020_billings,
                                    c20.dec_2020_arr,- - 2019 arr (
                                        if available
                                    ) c19.dec_2019_billings,
                                    c19.dec_2019_arr,- - 2018 arr (
                                        if available
                                    ) c18.dec_2018_billings,
                                    c18.dec_2018_arr,-- customer VALUE -- touch tier COALESCE(
                                        ctt.touch_tier,
                                        'Tech Touch (due to non-qualifying MRR)'
                                    ) AS touch_tier,-- contract TYPE cct.contract_type,-- it nation MEMBER (
                                        if available
                                    ) cr.itnation_active_partner,
                                    cr.itnation_peer_group_active_partner,-- qualitative churn scoring
                                FROM
                                    salesforce (
                                        if available
                                    ) chs.healthscore,
                                    chs.healthscore_alpha,-- overall tenure ct.corporate_start_date,
                                    ct.corporate_tenure,-- psa tenure ct.psa_start_date,
                                    ct.psa_tenure,
                                    cpp.psa_package,-- rmm tenure ct.rmm_start_date,
                                    ct.rmm_tenure,-- security tenure ct.security_start_date,
                                    ct.security_tenure,-- manage_active_partner cr.manage_active_partner,-- control_active_partner cr.control_active_partner,-- automate_active_partner cr.automate_active_partner,-- sell_active_partner cr.sell_active_partner,-- fortify_active_partner cr.fortify_active_partner,-- command_active_partner cr.command_active_partner,-- brightgauge_active_partner cr.brightgauge_active_partner,-- recover_active_partner cr.recover_active_partner,-- help desk_active_partner cr.help_desk_active_partner,-- security_active_partner cr.security_active_partner,-- itboost_active_partner cr.itboost_active_partner,-- noc_active_partner -- soc_active_partner -- psa seats served cr.psa_units,-- rmm endpoints served cr.rmm_units,
                                    cr.command_desktop_units,
                                    cr.command_network_units,
                                    cr.command_server_units,
                                    cr.help_desk_units,-- security endpoints served cr.security_units,
                                    ROUND(
                                        cr.psa_arr
                                    ) AS psa_arr,
                                    ROUND(
                                        cr.sell_arr
                                    ) AS sell_arr,
                                    ROUND(
                                        cr.brightgauge_arr
                                    ) AS brightgauge_arr,
                                    ROUND(
                                        cr.itboost_arr
                                    ) AS itboost_arr,
                                    ROUND(
                                        cr.rmm_arr
                                    ) AS rmm_arr,
                                    ROUND(
                                        cr.rmm_additional_arr
                                    ) AS rmm_additional_arr,
                                    ROUND(
                                        cr.help_desk_arr
                                    ) AS help_desk_arr,
                                    ROUND(
                                        cr.security_arr
                                    ) AS security_arr,
                                    ROUND(
                                        cr.other_arr
                                    ) AS other_arr,
                                    cr.psa_cloud,
                                    cr.psa_legacy_on_prem,
                                    cr.psa_on_prem,
                                    cr.sell_cloud,
                                    cr.sell_legacy_on_prem,
                                    cr.sell_on_prem,
                                    cr.brightgauge_cloud,
                                    cr.brightgauge_legacy_on_prem,
                                    cr.brightgauge_on_prem,
                                    cr.itboost_cloud,
                                    cr.itboost_legacy_on_prem,
                                    cr.itboost_on_prem,
                                    cr.rmm_cloud,
                                    cr.rmm_legacy_on_prem,
                                    cr.rmm_on_prem,
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
                                    psa_cloud_arr,
                                    psa_on_prem_arr,
                                    psa_legacy_on_prem_arr,
                                    automate_cloud_arr,
                                    automate_on_prem_arr,
                                    automate_legacy_on_prem_arr,
                                    control_cloud_arr,
                                    control_on_prem_arr,
                                    psa_cloud_units,
                                    psa_on_prem_units,
                                    psa_legacy_on_prem_units,
                                    automate_cloud_units,
                                    automate_on_prem_units,
                                    automate_legacy_on_prem_units
                                FROM
                                    customer_roster cr
                                    LEFT JOIN customer_2020_stats c20
                                    ON c20.company_name_with_id = cr.company_name_with_id
                                    LEFT JOIN customer_2019_stats c19
                                    ON c19.company_name_with_id = cr.company_name_with_id
                                    LEFT JOIN customer_2018_stats c18
                                    ON c18.company_name_with_id = cr.company_name_with_id
                                    LEFT JOIN customer_healthscores chs
                                    ON chs.company_id = cr.company_id
                                    LEFT JOIN customer_touch_tier ctt
                                    ON ctt.applied_date = cr.reporting_date
                                    AND ctt.ship_to = cr.company_id
                                    LEFT JOIN customer_contract_type cct
                                    ON cct.applied_date = cr.reporting_date
                                    AND cct.ship_to = cr.company_id
                                    LEFT JOIN customer_psa_package cpp
                                    ON cpp.company_name_with_id = cr.company_name_with_id
                                    LEFT JOIN customer_tenure ct
                                    ON ct.company_id = cr.company_id
