with base as (
    select
        *
    from
        DATAIKU.DEV_DATAIKU_WRITE.CWRMM_MIGRATION_STATUS
    where
        last_reporting_date_fill = 1
),
automate_migrated as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM', 'Churned Automate')
                and TABLE_FILTER = 'Automate to CWRMM' then coalesce(PRE_MIGRATION_OLD_PROD_ARR, OLD_PROD_MRR * 12, 0)
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM', 'Churned Automate')
                and TABLE_FILTER = 'Automate to CWRMM' then coalesce(PRE_MIGRATION_OLD_PROD_UNITS, OLD_PROD_UNITS, 0)
                else 0
            end
        ) as migration_walk_measure_units,
        'Automate Migrated' as migration_walk_dimension_arr,
        'Automate Migrated' as migration_walk_dimension_units,
        1 as sort_sequence
    from
        base
    group by
        1,
        2
),
command_migrated as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM', 'Churned Command')
                and TABLE_FILTER = 'Command to CWRMM' then coalesce(PRE_MIGRATION_OLD_PROD_ARR, OLD_PROD_MRR * 12, 0)
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM', 'Churned Command')
                and TABLE_FILTER = 'Command to CWRMM' then coalesce(PRE_MIGRATION_OLD_PROD_UNITS, OLD_PROD_UNITS, 0)
                else 0
            end
        ) as migration_walk_measure_units,
        'Command Migrated' as migration_walk_dimension_arr,
        'Command Migrated' as migration_walk_dimension_units,
        2 as sort_sequence
    from
        base
    group by
        1,
        2
),
total_migrated_arr as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in (
                    'Migrated to RMM',
                    'Churned Automate',
                    'Migrated to RMM',
                    'Churned Command'
                )
                and TABLE_FILTER in ('Command to CWRMM', 'Automate to CWRMM') then coalesce(PRE_MIGRATION_OLD_PROD_ARR, OLD_PROD_MRR * 12, 0)
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in (
                    'Migrated to RMM',
                    'Churned Automate',
                    'Migrated to RMM',
                    'Churned Command'
                )
                and TABLE_FILTER in ('Command to CWRMM', 'Automate to CWRMM') then coalesce(PRE_MIGRATION_OLD_PROD_UNITS, OLD_PROD_UNITS, 0)
                else 0
            end
        ) as migration_walk_measure_units,
        'Total Migrated ARR' as migration_walk_dimension_arr,
        'Total Migrated Units' as migration_walk_dimension_units,
        3 as sort_sequence
    from
        base
    group by
        1,
        2
),
automate_uplift as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM')
                and TABLE_FILTER in ('Automate to CWRMM') then PRE_MIG_MRR_DIFFERENCE * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM')
                and TABLE_FILTER in ('Automate to CWRMM') then PRE_MIG_UNITS_DIFFERENCE
                else 0
            end
        ) as migration_walk_measure_units,
        'Automate Uplift' as migration_walk_dimension_arr,
        'Automate Uplift' as migration_walk_dimension_units,
        4 as sort_sequence
    from
        base
    group by
        1,
        2
),
command_uplift as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM')
                and TABLE_FILTER in ('Command to CWRMM') then PRE_MIG_MRR_DIFFERENCE * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM')
                and TABLE_FILTER in ('Command to CWRMM') then PRE_MIG_UNITS_DIFFERENCE
                else 0
            end
        ) as migration_walk_measure_units,
        'Command Uplift' as migration_walk_dimension_arr,
        'Command Uplift' as migration_walk_dimension_units,
        5 as sort_sequence
    from
        base
    group by
        1,
        2
),
gross_arr as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM') then RMM_MRR * 12
                else 0
            end
        ) + sum(
            case
                when MIGRATION_STATUS in ('Churned Automate', 'Churned Command') then (OLD_PROD_MRR) * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM') then RMM_UNITS
                else 0
            end
        ) + sum(
            case
                when MIGRATION_STATUS in ('Churned Automate', 'Churned Command') then (OLD_PROD_UNITS)
                else 0
            end
        ) as migration_walk_measure_units,
        'Gross ARR' as migration_walk_dimension_arr,
        'Gross Units' as migration_walk_dimension_units,
        6 as sort_sequence
    from
        base
    group by
        1,
        2
),
automate_churn as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        - sum(
            case
                when MIGRATION_STATUS in ('Churned Automate') then OLD_PROD_MRR * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        - sum(
            case
                when MIGRATION_STATUS in ('Churned Automate') then OLD_PROD_UNITS
                else 0
            end
        ) as migration_walk_measure_units,
        'Automate Churn' as migration_walk_dimension_arr,
        'Automate Churn' as migration_walk_dimension_units,
        7 as sort_sequence
    from
        base
    group by
        1,
        2
),
command_churn as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        - sum(
            case
                when MIGRATION_STATUS in ('Churned Command') then OLD_PROD_MRR * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        - sum(
            case
                when MIGRATION_STATUS in ('Churned Command') then OLD_PROD_UNITS
                else 0
            end
        ) as migration_walk_measure_units,
        'Command Churn' as migration_walk_dimension_arr,
        'Command Churn' as migration_walk_dimension_units,
        8 as sort_sequence
    from
        base
    group by
        1,
        2
),
rmm_arr as(
    select
        COMPANY_ID,
        REPORTING_DATE,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM') then RMM_MRR * 12
                else 0
            end
        ) as migration_walk_measure_arr,
        sum(
            case
                when MIGRATION_STATUS in ('Migrated to RMM') then RMM_UNITS
                else 0
            end
        ) as migration_walk_measure_units,
        'New ARR' as migration_walk_dimension_arr,
        'New Units' as migration_walk_dimension_units,
        9 as sort_sequence
    from
        base
    group by
        1,
        2
)
select
    *
from
    automate_migrated
union
select
    *
from
    command_migrated
union
select
    *
from
    total_migrated_arr
union
select
    *
from
    automate_uplift
union
select
    *
from
    command_uplift
union
select
    *
from
    gross_arr
union
select
    *
from
    automate_churn
union
select
    *
from
    command_churn
union
select
    *
from
    rmm_arr