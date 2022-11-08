with new_packages as(
    select distinct  COMPANY_ID,
                     min(COMPANY_NAME) as COMPANY_NAME,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='CW RMM',
                         1,0)) as has_cwrmm_mrr,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='Business Mgmt Packages',
                         1,0)) as has_bms_mrr,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='CW RMM',
                         Units,0)) as cwrmm_Units,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='CW RMM',
                         MRR,0)) as cwrmm_mrr,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='Business Mgmt Packages',
                         Units,0)) as bms_Units,
                     max(iff (obt.PRODUCT_CATEGORIZATION_PRODUCT_LINE='Business Mgmt Packages',
                         MRR,0)) as bms_mrr

    from ANALYTICS.DBO.GROWTH__OBT obt
--    from analytics_dev.dbt_ag.Growth__obt  obt

    where REPORTING_DATE = (select distinct
                            case
                            when day(CURRENT_DATE()) > 2
                            then date_trunc('Month',add_months(CURRENT_DATE()::date, -1))
                            else date_trunc('Month',add_months(CURRENT_DATE()::date, -2))
                            end as date)
    and METRIC_OBJECT = 'applied_billings'
    and obt.mrr>0
    group by 1
),
customer_roster AS (
    SELECT REPORTING_DATE,
--           COMPANY_NAME_WITH_ID,
           obt.COMPANY_ID,
           min(obt.COMPANY_NAME) as COMPANY_NAME,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND BILLINGS > 0, 1,
                   0))                                                                   AS manage_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Control' AND BILLINGS > 0, 1,
                   0))                                                                   AS control_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Automate' AND BILLINGS > 0, 1,
                   0))                                                                   AS automate_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell' AND BILLINGS > 0, 1, 0)) AS sell_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Fortify' AND BILLINGS > 0, 1,
                   0))                                                                   AS fortify_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Command' AND BILLINGS > 0, 1,
                   0))                                                                   AS command_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge' AND BILLINGS > 0, 1,
                   0))                                                                   AS brightgauge_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Recover' AND BILLINGS > 0, 1,
                   0))                                                                   AS recover_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk' AND BILLINGS > 0, 1,
                   0))                                                                   AS help_desk_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost' AND BILLINGS > 0, 1,
                   0))                                                                   AS itboost_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Perch' AND BILLINGS > 0, 1,
                   0))                                                                   AS security_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'IT Nation' AND BILLINGS > 0, 1,
                   0))                                                                   AS itnation_active_partner,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'IT Nation' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Evolve', 'ITN Evolve') AND BILLINGS > 0, 1,
                   0))                                                                   AS itnation_peer_group_active_partner,
--            SUM(IFF(ITEM_ID in (
--                 'MNH-UKM-A00-LAITTBASAS',
--                 'MNH-USM-A00-LAITTBASAS',
--                 'MNLICONPREMBMNPRMSUB',
--                 'MNLICONPREMBMNSTDSUB',
--                 'MNG-LIC-SAASMNGBASAS',
--                 'MNG-LIC-SAASITTBASIC',
--                 'MNLICONPREMBITTBASIC',
--                 'MNG-LIC-SAASITTSTAND',
--                 'MNLICONPREMBITTSTDSB',
--                 'MNG-LIC-SAASMNPRMSAS',
--                 'MNG-LIC-SAASMNGSTDSS',
--                 'MNLICONPREMBCWUSERIN',
--                 'MNG-LIC-SAASCWUSERIN',
--                 'MNLICONPREMBCWUSERPR',
--                 'MNG-LIC-SAASCWUSERNM',
--                 'MNG-LIC-SAASCLDCHNAM',
--                 'LEGACYPSACloudChnnel',
--                 'LEGACYSTANDUSERSTND',
--                 'MNLICONPREMBSASPLNSV',
--                 'MNLICONPREMPCWUSERIN',
--                 'MNSASMAININTNAMEUSER',
--                 'MNSASMAINADDNAMEUSER',
--                 'LEGACYPSA-ASSURANCEA',
--                 'LEGACYSKUCLDCHANN',
--                 'MNMAINPRPADDINCLUSER',
--                 'LEGACYSKUAIINUSRSTD',
--                 'MNLICONPREMPCWUSERNM',
--                 'LEGACYPSA-ASSURANCEM',
--                 'LEGACYMACCONBASQ-Q00',
--                 'LEGACYLACWUSERINM01',
--                 'MNMAINPRPINTINCLUSER',
--                 'LEGACYPSA-ASSURANCEQ'
--                 ),
--                    UNITS, 0))                                                            AS PSA_UNITS,
            max(iff(PRODUCT_CATEGORIZATION_PRODUCT_LINE='Manage'
                and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ='Manage'
                and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-','Basic','Standard','Premium')
                and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in ('SaaS', 'Maintenance', 'On Premise (Subscription)')
                and has_bms_mrr <>1,
                Units,0)) as PSA_UNITS,

--            MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
--                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops','Networks',                                                             'Servers'), UNITS,
--                    0))                                                                   AS RMM_UNITS,

           MAX(IFF( (PRODUCT_CATEGORIZATION_PRODUCT_LINE='Automate'
                    and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE ='Automate'
                    and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-', 'Standard','Internal IT')
                    and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in ('SaaS', 'Maintenance', 'On Premise (Subscription)')
                    and has_cwrmm_mrr <>1), UNITS,0))
           + SUM(IFF (PRODUCT_CATEGORIZATION_PRODUCT_LINE ='Command' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops','Servers')
                   and has_cwrmm_mrr <>1, UNITS,0))

               AS RMM_UNITS,


           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops'), UNITS,
                   0))                                                                   AS COMMAND_DESKTOP_UNITS,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Networks'), UNITS,
                   0))                                                                   AS COMMAND_NETWORK_UNITS,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Servers'), UNITS,
                   0))                                                                   AS COMMAND_SERVER_UNITS,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Help Desk'), UNITS, 0))      AS HELP_DESK_UNITS,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', UNITS,
                   0))                                                                   AS SECURITY_UNITS,
           ROUND(SUM(BILLINGS))                                                          AS CURRENT_BILLINGS,
           ROUND(SUM(ARR))                                                               AS CURRENT_ARR,

        --    as part of change on 04/04 based on inputs from Pricing and packaging team
        --    SUM(IFF(ITEM_ID in (
        --         'MNLICONPREMBCLDCNNC3',
        --         'MNG-LIC-SAASCLDCNNCT',
        --         'LEGACYLBCLDCNNCTM00',
        --         'LEGACYMCLDRPRT2CLDR',
        --         'MNLICSAASMODBDXMEGAA',
        --         'MNLICONPREMBMNPRMSUB',
        --         'CWH-USC-M00-ABMNGEPREM',
        --         'CWH-USD-M00-ABMNGEPREM',
        --         'CWH-UKD-M00-ABMNGEPREM',
        --         'MNLICONPREMBMNSTDSUB',
        --         'MNLICONPREMBCWUSERNM',
        --         'LEGACYSKUCWUSERNM',
        --         'MNLICONPREMBTESTENVI',
        --         'MNG-LIC-SAASCLDDTACC',
        --         'MNLICONPREMBCWPSAPRI',
        --         'MNG-LIC-SAASMNGBASAS',
        --         'MNG-LIC-SAASMNGCHGMT',
        --         'MNG-LIC-SAASITTBASIC',
        --         'MNH-UKM-A00-LAITTBASAS',
        --         'MNH-USM-A00-LAITTBASAS',
        --         'MNLICONPREMBITTBASIC',
        --         'MNH-USM-A00-LBITTBASUB',
        --         'MNG-LIC-SAASITTSTAND',
        --         'MNLICONPREMBITTSTDSB',
        --         'MNG-LIC-SAASCWUSERMO',
        --         'PSA - Monthly Mobile',
        --         'MNLICONPREMBCWUSERMO',
        --         'MNLICSAASMODBDXBIGBX',
        --         'MNLICONPREMBEMAILCON',
        --         'MNLICONPREMBEMAILCON',
        --         'MNLICSAASMODEMAILC02',
        --         'MNLICONPREMBMSPCONCT',
        --         'MNLICSAASMODMSPCONCT',
        --         'LEGACYSKUPROINVRY',
        --         'MNLICONPREMBPROINVRY',
        --         'MNLICSAASMODPROINVRY',
        --         'MNLICONPREMBPROCRMNT',
        --         'MNLICSAASMODPROCRMNT',
        --         'MNG-LIC-SAASCWUSRFEE',
        --         'MNG-LIC-SAASMNPRMSAS',
        --         'MNLICONPREMBSANDBOXB',
        --         'MNG-LIC-SAASSSANDBOX',
        --         'MNLICONPREMBTESTOMPM',
        --         'MNLICONPREMBSANDBOXS',
        --         'MNG-LIC-SAASMNGSTDSS',
        --         'MNLICONPREMBMNSVRFEE',
        --         'AULICONPREMMNSVRFEE',
        --         'MNLICONPREMBCWUSRCRM',
        --         'MNG-LIC-SAASCWUSRCRM',
        --         'MNLICONPREMBCWUSERIN',
        --         'MNG-LIC-SAASCWUSERIN',
        --         'MNLICONPREMBCWUSERPR',
        --         'MNG-LIC-SAASCWUSERNM',
        --         'MNG-LIC-SAASCWUSERTO',
        --         'MNLICONPREMBCWUSRSCH',
        --         'MNG-LIC-SAASCWUSRSCH',
        --         'MNG-LIC-SAASCLDCHNAM',
        --         'LEGACYPSACloudChnnel',
        --         'LEGACYSTANDUSERSTND',
        --         'MNLICONPREMBSASPLNSV',
        --         'LEGACYPSASaaSChannel',
        --         'LEGACYPSASAASPLANSEV',
        --         'MNLICONPREMPCWUSERIN',
        --         'MNSASMAININTNAMEUSER',
        --         'MNSASMAINADDNAMEUSER',
        --         'LEGACYPSA-ASSURANCEA',
        --         'LEGACYSKUCLDCHANN',
        --         'MNMAINPRPADDINCLUSER',
        --         'LEGACYSKUAIINUSRSTD',
        --         'MNLICONPREMPCWUSERNM',
        --         'LEGACYPSA-ASSURANCEM',
        --         'LEGACYMACCONBASQ-Q00',
        --         'LEGACYLACWUSERINM01',
        --         'MNMAINPRPINTINCLUSER',
        --         'LEGACYPSA-ASSURANCEQ',
        --         'LEGACYONPREMMONTHLY'),
        --            ARR, 0))                                                            AS PSA_ARR,

            -- A.H. October, 2022: Short term solution to get all the ARR for Manage
            SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE='Manage'
                    and PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE='Manage'
                 --   and PRODUCT_CATEGORIZATION_PRODUCT_PLAN in ('-','Basic','Standard','Premium')
                    and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in ('SaaS', 'Maintenance', 'On Premise (Subscription)')
                    and has_bms_mrr <>1,ARR,0))                                          AS PSA_ARR,

           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell', ARR, 0))                AS SELL_ARR,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge', ARR, 0))         AS BRIGHTGAUGE_ARR,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost', ARR, 0))             AS ITBOOST_ARR,
--            SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
--                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers'), ARR,
--                    0))                                                                   AS RMM_ARR,
           SUM(IFF( PRODUCT_CATEGORIZATION_PRODUCT_LINE='Automate'
                    and PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE in ('SaaS', 'Maintenance', 'On Premise (Subscription)')
                    and has_cwrmm_mrr <>1, ARR,0))
           + SUM(IFF (PRODUCT_CATEGORIZATION_PRODUCT_LINE ='Command' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Desktops','Servers')
                   and has_cwrmm_mrr <>1, ARR,0))                                      AS RMM_ARR,

           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Third Party Patching'), ARR,
                   0))                                                                   AS RMM_ADDITIONAL_ARR,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk', ARR, 0))           AS HELP_DESK_ARR,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS' AND
                   lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) <> 'webroot', ARR,
                   0))                                                                   AS SECURITY_ARR,
           SUM(IFF(
                           PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN
                                ('Automate', 'Desktops', 'Networks', 'Servers'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                                PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'),
                           ARR,
                           0)
               )                                                                         AS OTHER_ARR,


           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                   1, 0))                                                                AS PSA_CLOUD,

           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1, 0))          AS SELL_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1,
                   0))                                                                   AS BRIGHTGAUGE_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1,
                   0))                                                                   AS ITBOOST_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1,
                   0))                                                                   AS RMM_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk', 1, 0))             AS HELP_DESK_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS'
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', 1,
                   0))                                                                   AS SECURITY_CLOUD,
           MAX(IFF(
                           PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN
                                ('Automate', 'Desktops', 'Networks', 'Servers'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                                PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                           1,
                           0)
               )                                                                         AS OTHER_CLOUD,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                   1,
                   0))                                                                   AS PSA_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), ARR,
                   0))                                                                   AS SELL_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), ARR,
                   0))                                                                   AS BRIGHTGAUGE_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), ARR,
                   0))                                                                   AS ITBOOST_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), 1,
                   0))                                                                   AS RMM_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk', 1, 0))             AS HELP_DESK_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), 1,
                   0))                                                                   AS SECURITY_LEGACY_ON_PREM,
           MAX(IFF(
                           PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN
                                ('Automate', 'Desktops', 'Networks', 'Servers'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                                PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                           1,
                           0)
               )                                                                         AS OTHER_LEGACY_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                   1,
                   0))                                                                   AS PSA_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Sell' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), ARR,
                   0))                                                                   AS SELL_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'BrightGauge' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), ARR,
                   0))                                                                   AS BRIGHTGAUGE_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'ITBoost' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), ARR,
                   0))                                                                   AS ITBOOST_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), 1,
                   0))                                                                   AS RMM_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Help Desk', 1, 0))             AS HELP_DESK_ON_PREM,
           MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                   PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), 1,
                   0))                                                                   AS SECURITY_ON_PREM,
           MAX(IFF(
                           PRODUCT_CATEGORIZATION_PRODUCT_LINE NOT IN ('Sell', 'BrightGauge', 'ITBoost', 'Help Desk')
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN
                                ('Automate', 'Desktops', 'Networks', 'Servers'))
                       AND NOT (PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Network & Endpoint Security' AND
                                PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
                           1,
                           0)
               )                                                                         AS OTHER_ON_PREM,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE LIKE '%Solution Partners%'
                       AND lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) <> 'webroot', MRR,
                   0))                                                                   AS THIRD_PARTY_MRR,
           SUM(IFF(lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot' AND ITEM_ID <> '3P-SAAS3002315EPPRMM', MRR,
                   0))                                                                   AS WEBROOT_MRR,
           SUM(IFF(lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot' AND ITEM_ID <> '3P-SAAS3002315EPPRMM',
                   UNITS,
                   0))                                                                   AS WEBROOT_UNITS,
           SUM(IFF(lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot' AND ITEM_ID = '3P-SAAS3002315EPPRMM', MRR,
                   0))                                                                   AS WEBROOT_OVERAGE_MRR,
           SUM(IFF(lower(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE) = 'webroot' AND ITEM_ID = '3P-SAAS3002315EPPRMM',
                   UNITS,
                   0))                                                                   AS WEBROOT_OVERAGE_UNITS,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                   ARR, 0)) AS psa_cloud_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
               ARR, 0)) AS psa_on_prem_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                   ARR, 0)) AS psa_legacy_on_prem_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', arr,
                   0)) AS automate_cloud_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), arr,
                   0)) AS automate_on_prem_arr,
               SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), arr,
                   0)) AS automate_legacy_on_prem_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Control' AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', ARR, 0)) AS control_cloud_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Control' AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), ARR, 0)) AS control_on_prem_arr,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS',
                   units, 0)) AS psa_cloud_units,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'),
               units, 0)) AS psa_on_prem_units,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('-', 'Standard', 'Premium', 'Basic')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'),
                   units, 0)) AS psa_legacy_on_prem_units,
            SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate') AND
                    PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate')
                        AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE = 'SaaS', units,
                            0)) AS automate_cloud_units,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('On Premise (Subscription)'), units,
                   0)) AS automate_on_prem_units,
           SUM(IFF(PRODUCT_CATEGORIZATION_PRODUCT_LINE IN ('Automate', 'Command') AND
                   PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE IN ('Automate', 'Desktops', 'Networks', 'Servers')
                       AND PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE IN ('Perpetual', 'Maintenance'), units,
                   0)) AS automate_legacy_on_prem_units

    from ANALYTICS.DBO.GROWTH__OBT obt
--     from analytics_dev.dbt_ag.Growth__obt  obt
left join new_packages np
    on np.COMPANY_ID=obt.COMPANY_ID
    WHERE --REPORTING_DATE='2022-09-01'
        REPORTING_DATE = (select distinct
                            case
                            when day(CURRENT_DATE()) > 2
                            then date_trunc('Month',add_months(CURRENT_DATE()::date, -1))
                            else date_trunc('Month',add_months(CURRENT_DATE()::date, -2))
                            end as date)
--             year(REPORTING_DATE)>2020
--        and
--         obt.COMPANY_NAME ilike '%revium%' --or
--         COMPANY_NAME ilike '%Hubwise%'
    -- Change done as part of Sprint 9, it's to make date dynamic based on current date to bring legacy Continumm and Connectwise date at same level by date
    -- Change initiated by Carl
    -- DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE)) -- Filter to refresh data YYYY-MM-DD
      AND METRIC_OBJECT = 'applied_billings'
           AND obt.Company_name <> ''
    GROUP BY 1, 2
    HAVING SUM(BILLINGS) > 0


),
       customer_2021_stats AS (
         SELECT COMPANY_ID,
                ROUND(SUM(BILLINGS)) AS DEC_2021_BILLINGS,
                ROUND(SUM(ARR))      AS DEC_2021_ARR
         FROM ANALYTICS.DBO.GROWTH__OBT
         WHERE REPORTING_DATE = '2021-12-01'
           AND METRIC_OBJECT = 'applied_billings'
         GROUP BY 1
         HAVING SUM(BILLINGS) > 0
     ),

     customer_2020_stats AS (
         SELECT COMPANY_ID,
                ROUND(SUM(BILLINGS)) AS DEC_2020_BILLINGS,
                ROUND(SUM(ARR))      AS DEC_2020_ARR
         FROM ANALYTICS.DBO.GROWTH__OBT
         WHERE REPORTING_DATE = '2020-12-01'
           AND METRIC_OBJECT = 'applied_billings'
         GROUP BY 1
         HAVING SUM(BILLINGS) > 0
     ),


     customer_2019_stats AS (
         SELECT COMPANY_ID,
                ROUND(SUM(BILLINGS)) AS DEC_2019_BILLINGS,
                ROUND(SUM(ARR))      AS DEC_2019_ARR
         FROM ANALYTICS.DBO.GROWTH__OBT
         WHERE REPORTING_DATE = '2019-12-01'
           AND METRIC_OBJECT = 'applied_billings'
         GROUP BY 1
         HAVING SUM(BILLINGS) > 0
     ),

    contract as
        (
            with fl as
                (
            select COMPANY_ID, CONTRACT_NUMBER, PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT ,
                   min(START_DATE) as START_DATE,
                   max(END_DATE) as END_DATE
                from ANALYTICS.DBO.GROWTH__OBT
                    where METRIC_OBJECT='renewals'
                    group by 1,2,3),
            sl as
            (select *,
                   IFF(END_DATE < '2022-01-01'::date, '2099-01-01'::date, END_DATE) as dayfilter,
                   min(dayfilter) OVER ( PARTITION BY COMPANY_ID order by COMPANY_ID) AS nearestdate,
                   IFF(nearestdate = END_DATE, 1, 0) as daysfilterflag
            from fl
            where END_DATE>
                 --CAST( GETDATE() AS Date )
                          (select distinct
                            case
                            when day(CURRENT_DATE()) > 2
                            then date_trunc('Month',add_months(CURRENT_DATE()::date, 0))
                            else date_trunc('Month',add_months(CURRENT_DATE()::date, -1))
                            end as date)
            )
            select COMPANY_ID,END_DATE as Earliest_Date, listagg(CONTRACT_NUMBER,',') as CONTRACT_NUMBER, listagg(PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT,',') as Products
                from sl where daysfilterflag=1 group by 1,2
        ),

--      customer_2018_stats AS (
--          SELECT COMPANY_NAME_WITH_ID,
--                 ROUND(SUM(BILLINGS)) AS DEC_2018_BILLINGS,
--                 ROUND(SUM(ARR))      AS DEC_2018_ARR
--          FROM ANALYTICS.DBT_BAW.GROWTH__OBT
--          WHERE REPORTING_DATE = '2018-12-01'
--            AND METRIC_OBJECT = 'applied_billings'
--          GROUP BY 1
--          HAVING SUM(BILLINGS) > 0
--      ),

     customer_healthscores AS (
         SELECT SHIP_TO AS COMPANY_ID,
                HEALTHSCORE,
                HEALTHSCORE_ALPHA
         FROM ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__HEALTHSCORES
             QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIP_TO ORDER BY APDATE DESC) = 1
     ),

     customer_touch_tier AS (
         SELECT APPLIED_DATE,
                SHIP_TO,
                COUNT(DISTINCT TOUCH_TIER)                                               AS TT_COUNT,
                LISTAGG(DISTINCT TOUCH_TIER, ' | ') WITHIN GROUP ( ORDER BY TOUCH_TIER ) AS TT_CLASSES,
                MIN(TOUCH_TIER)                                                          AS TT_MIN,
                MAX(TOUCH_TIER)                                                          AS TT_MAX,
                MIN(TOUCH_TIER)                                                          AS TOUCH_TIER
         FROM ANALYTICS.DBO_TRANSFORMATION.PARTNER_SUCCESS_INTERMEDIATE__TECH_TOUCH_ROSTER
         GROUP BY 1, 2
     ),
     customer_contract_type AS (
         SELECT APPLIED_DATE,
                SHIP_TO,
                LISTAGG(DISTINCT COALESCE(QUOTE_LINE_SUBSCRIPTION_TYPE, 'Non-Bedrock M2M'), ' | ')
                        WITHIN GROUP ( ORDER BY COALESCE(QUOTE_LINE_SUBSCRIPTION_TYPE, 'Non-Bedrock M2M') ) AS CONTRACT_TYPE
         FROM ANALYTICS.dbo.CORE__RPT_BILLINGS
         GROUP BY 1, 2
     ),

     customer_psa_package AS (
         SELECT COMPANY_ID,
--                 COMPANY_NAME_WITH_ID,
                LISTAGG(DISTINCT IFF(PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                     PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('Basic', 'Standard', 'Premium'),
                                     PRODUCT_CATEGORIZATION_PRODUCT_PLAN, 'Legacy'), ' | ') WITHIN GROUP ( ORDER BY IFF(
                                PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE = 'Manage' AND
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN IN ('Basic', 'Standard', 'Premium'),
                                PRODUCT_CATEGORIZATION_PRODUCT_PLAN, 'Legacy') ) AS PSA_PACKAGE
         FROM ANALYTICS.DBO.GROWTH__OBT
         WHERE REPORTING_DATE = (select distinct
                            case
                            when day(CURRENT_DATE()) > 2
                            then date_trunc('Month',add_months(CURRENT_DATE()::date, -1))
                            else date_trunc('Month',add_months(CURRENT_DATE()::date, -2))
                            end as date)
        -- DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE))  -- Filter to refresh data YYYY-MM-DD
           AND METRIC_OBJECT = 'applied_billings'
           AND PRODUCT_CATEGORIZATION_PRODUCT_LINE = 'Manage'
           AND BILLINGS > 0
         GROUP BY 1
     ),

     customer_tenure AS (
         SELECT COMPANY_ID,
                MIN(CORPORATE_BILLING_START_DATE)                          AS CORPORATE_START_DATE,
                MAX(CORPORATE_BILLING_REPORTING_PERIOD)                    AS CORPORATE_TENURE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Client & Process Mgmt',
                        PRODUCT_GROUP_BILLING_START_DATE,
                        NULL))                                             AS PSA_START_DATE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Client & Process Mgmt',
                        PRODUCT_GROUP_BILLING_REPORTING_PERIOD, NULL))     AS PSA_TENURE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Remote Monitoring & Mgmt',
                        PRODUCT_GROUP_BILLING_START_DATE,
                        NULL))                                             AS RMM_START_DATE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_GROUP = 'Remote Monitoring & Mgmt',
                        PRODUCT_GROUP_BILLING_REPORTING_PERIOD, NULL))     AS RMM_TENURE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PORTFOLIO = 'Security Mgmt',
                        PRODUCT_PORTFOLIO_BILLING_START_DATE,
                        NULL))                                             AS SECURITY_START_DATE,
                MAX(IFF(PRODUCT_CATEGORIZATION_PRODUCT_PORTFOLIO = 'Security Mgmt',
                        PRODUCT_PORTFOLIO_BILLING_REPORTING_PERIOD, NULL)) AS SECURITY_TENURE
         FROM analytics.DBO.growth__obt
         GROUP BY 1
     )
SELECT
    -- Customer ID
    cr.COMPANY_ID,
    cr.COMPANY_NAME,
    concat(cr.COMPANY_ID,cr.COMPANY_NAME) as COMPANY_NAME_WITH_ID,
    -- Current ARR
    cr.CURRENT_ARR,
    -- 2020 ARR (if available)
    c21.DEC_2021_BILLINGS,
    c21.DEC_2021_ARR,
    -- 2019 ARR (if available)
    c20.DEC_2020_BILLINGS,
    c20.DEC_2020_ARR,
    -- 2018 ARR (if available)
    c19.DEC_2019_BILLINGS,
    c19.DEC_2019_ARR,
    -- Customer Value -- touch tier
    COALESCE(ctt.TOUCH_TIER, 'Tech Touch (due to non-qualifying MRR)') AS TOUCH_TIER,
    -- Contract Type
    cct.CONTRACT_TYPE,
    -- IT Nation Member (if available)
--     cr.itnation_active_partner, -- not present in raw data, hence commented
    cr.itnation_peer_group_active_partner,
    -- Qualitative Churn Scoring from Salesforce (if available)
    chs.HEALTHSCORE,
    chs.HEALTHSCORE_ALPHA,
    -- Overall Tenure
    ct.CORPORATE_START_DATE,
    ct.CORPORATE_TENURE,
    -- PSA Tenure
    ct.PSA_START_DATE,
    ct.PSA_TENURE,
    cpp.PSA_PACKAGE,
    -- RMM Tenure
    ct.RMM_START_DATE,
    ct.RMM_TENURE,
    -- Security Tenure
    ct.SECURITY_START_DATE,
    ct.SECURITY_TENURE,
    -- Manage_active_partner
    cr.manage_active_partner,
    -- Control_active_partner
    cr.control_active_partner,
    -- Automate_active_partner
    cr.automate_active_partner,
    -- Sell_active_partner
    cr.sell_active_partner,
    -- Fortify_active_partner
    cr.fortify_active_partner,
    -- Command_active_partner
    cr.command_active_partner,
    -- BrightGauge_active_partner
    cr.brightgauge_active_partner,
    -- Recover_active_partner
    cr.recover_active_partner,
    -- Help Desk_active_partner
    cr.help_desk_active_partner,
    -- Security_active_partner
    cr.security_active_partner,
    -- ITBoost_active_partner
    cr.itboost_active_partner,
    -- NOC_active_partner
    -- SOC_active_partner
    -- PSA Seats Served
    cr.PSA_UNITS,
    -- RMM Endpoints Served
    cr.RMM_UNITS,
    cr.COMMAND_DESKTOP_UNITS,
    cr.COMMAND_NETWORK_UNITS,
    cr.COMMAND_SERVER_UNITS,
    cr.HELP_DESK_UNITS,
    -- Security Endpoints Served
    cr.SECURITY_UNITS,
    ROUND(cr.PSA_ARR)                                                  AS PSA_ARR,
    ROUND(cr.SELL_ARR)                                                 AS SELL_ARR,
    ROUND(cr.BRIGHTGAUGE_ARR)                                          AS BRIGHTGAUGE_ARR,
    ROUND(cr.ITBOOST_ARR)                                              AS ITBOOST_ARR,
    ROUND(cr.RMM_ARR)                                                  AS RMM_ARR,
    ROUND(cr.RMM_ADDITIONAL_ARR)                                       AS RMM_ADDITIONAL_ARR,
    ROUND(cr.HELP_DESK_ARR)                                            AS HELP_DESK_ARR,
    ROUND(cr.SECURITY_ARR)                                             AS SECURITY_ARR,
    ROUND(cr.OTHER_ARR)                                                AS OTHER_ARR,
    cr.PSA_CLOUD,
    cr.PSA_LEGACY_ON_PREM,
    cr.PSA_ON_PREM,
    cr.SELL_CLOUD,
    cr.SELL_LEGACY_ON_PREM,
    cr.SELL_ON_PREM,
    cr.BRIGHTGAUGE_CLOUD,
    cr.BRIGHTGAUGE_LEGACY_ON_PREM,
    cr.BRIGHTGAUGE_ON_PREM,
    cr.ITBOOST_CLOUD,
    cr.ITBOOST_LEGACY_ON_PREM,
    cr.ITBOOST_ON_PREM,
    cr.RMM_CLOUD,
    cr.RMM_LEGACY_ON_PREM,
    cr.RMM_ON_PREM,
    cr.HELP_DESK_CLOUD,
    cr.HELP_DESK_LEGACY_ON_PREM,
    cr.HELP_DESK_ON_PREM,
    cr.SECURITY_CLOUD,
    cr.SECURITY_LEGACY_ON_PREM,
    cr.SECURITY_ON_PREM,
    cr.OTHER_CLOUD,
    cr.OTHER_LEGACY_ON_PREM,
    cr.OTHER_ON_PREM,
    cr.THIRD_PARTY_MRR,
    cr.WEBROOT_MRR,
    cr.WEBROOT_UNITS,
    cr.WEBROOT_OVERAGE_MRR,
    cr.WEBROOT_OVERAGE_UNITS,
    psa_cloud_arr,
    psa_on_prem_arr,
    psa_legacy_on_prem_arr,
    automate_cloud_arr,
    automate_on_prem_arr,
    automate_legacy_on_prem_arr,
--     control_cloud_arr,
--     control_on_prem_arr,
    psa_cloud_units,
    psa_on_prem_units,
    psa_legacy_on_prem_units,
    automate_cloud_units,
    automate_on_prem_units,
    automate_legacy_on_prem_units,
    c.CONTRACT_NUMBER,
    c.Earliest_Date,
    c.Products
FROM customer_roster cr
         LEFT JOIN customer_2021_stats c21 ON c21.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN customer_2020_stats c20 ON c20.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN customer_2019_stats c19 ON c19.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN contract c ON c.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN customer_healthscores chs ON chs.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN customer_touch_tier ctt ON ctt.APPLIED_DATE = cr.REPORTING_DATE AND ctt.SHIP_TO = cr.COMPANY_ID
         LEFT JOIN customer_contract_type cct ON cct.APPLIED_DATE = cr.REPORTING_DATE AND cct.SHIP_TO = cr.COMPANY_ID
         LEFT JOIN customer_psa_package cpp ON cpp.COMPANY_ID = cr.COMPANY_ID
         LEFT JOIN customer_tenure ct ON ct.COMPANY_ID = cr.COMPANY_ID
