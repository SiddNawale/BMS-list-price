-- Project      Control Freemium Dev
-- Purpose      Looking at control partners who had free licenses between 10/21 and 12/22
--              and seeing what license they have currently, including how many times they have switched and when
--
-- Input        - Tables:
--                      -- InstanceChange
--                              -- license history: what license they had in the past, what they have been changing to and when
--                      -- reporting.InstancesWithActiveLicense
--                              --- Gets active control partners with what they currently have
-- Database     - MS SQL
-----------------------------------------------------------------------------------------------------------
with base as (
    SELECT
        DISTINCT InstanceID,
        LicenseTypeID,
        BillingStartDate
    FROM
        InstanceChange IC
    WHERE
        InstanceChangeID IN (
            SELECT
                MAX(InstanceChangeID)
            FROM
                InstanceChange
            WHERE
                InstanceID = IC.InstanceID
                AND Resolution = 1
                AND datetrunc(month, ResolutionTime) < '2021-10-01'
        )
        AND LicenseTypeID LIKE '%FREE%'
        AND LicenseCount > 0
    UNION
    ----First half of union gets the last tim that instance changed to free
    SELECT
        DISTINCT InstanceID,
        LicenseTypeID,
        BillingStartDate
    FROM
        InstanceChange
    WHERE
        LicenseTypeID LIKE '%FREE%'
        AND LicenseCount <> 0
        AND Resolution = 1
        AND ResolutionTime BETWEEN '2021-10-01'
        AND '2022-12-01'
),
--------this half of the union takes into account everytime the instance changed to free over the period of time we specified 
base_with_account as (
    select
        AccountID,
        base.LicenseTypeID,
        Instance.InstanceID,
        BillingStartDate
    from
        base
        left join Instance on base.InstanceID = Instance.InstanceID
),
changes as (
    SELECT
        InstanceID,
        InstanceChangeID,
        ResolutionTime,
        SUBSTRING(
            LicenseTypeID,
            0,
            charindex('-', LicenseTypeID, 0)
        ) LicenseType
    FROM
        InstanceChange
    WHERE
        LicenseTypeID IN (
            SELECT
                DISTINCT LicenseTypeID
            from
                LicensePrice
            WHERE
                LicenseTypeID != ' '
        )
        AND Resolution = 1
        and DATETRUNC(month, ResolutionTime) BETWEEN '2021-10-01'
        AND '2022-12-01'
),
--changes gets history of the licenses that each instanceid had in the timeframe we specified
instance_hops_and_dates as (
    SELECT
        InstanceID,
        cast(datetrunc(day, MIN(ResolutionTime)) as date) PurchaseDate,
        cast(datetrunc(month, MIN(ResolutionTime)) as date) PurchaseDate_month_trunc,
        string_agg(
            cast(DATETRUNC(day, ResolutionTime) as date),
            ','
        ) license_acquitsion_dates,
        string_agg(
            concat(
                datename(
                    year,
                    (cast(DATETRUNC(quarter, ResolutionTime) as date))
                ),
                '-',
                'Q',
                datename(
                    quarter,
                    (cast(DATETRUNC(quarter, ResolutionTime) as date))
                )
            ),
            ','
        ) as license_acquitsion_quarter,
        COUNT(DISTINCT LicenseType) LicenseHops,
        string_agg(LicenseType, ' | ') LicenseType,
        count(LicenseType) license_count
    FROM
        changes
    GROUP BY
        InstanceID
) ---combines all the license changes into one row per instanceid with dates and purchase orders
select
    base_with_account.accountid,
    base_with_account.InstanceID,
    base_with_account.licensetypeid as previous_license,
    al.accountid,
    al.InstanceID,
    al.licensetypeid as active_license,
    BillingStartDate,
    case
        when al.licensetypeid = 'FREE-1606' then null
        Else string_agg(cast(DATETRUNC(day, PurchaseDate) as date), ',')
    end as first_paid_purchase_date,
    --        PurchaseDate,
    case
        when al.licensetypeid = 'FREE-1606' then null
        else PurchaseDate_month_trunc
    end as first_paid_purchase_date_month_trunc,
    case
        when al.licensetypeid = 'FREE-1606' then null
        else license_acquitsion_dates
    end as license_dates,
    license_acquitsion_dates as license_acquitsion_dates_with_free,
    license_acquitsion_quarter,
    LicenseHops,
    LicenseType,
    license_count
from
    base_with_account
    left join (
        SELECT
            distinct AccountID,
            LicenseTypeID,
            InstanceID
        from
            reporting.InstancesWithActiveLicense al
        where
            LicenseCount > 0
    ) al on al.accountID = base_with_account.AccountID
    and al.InstanceID = base_with_account.InstanceID
    left join instance_hops_and_dates ins on ins.InstanceID = base_with_account.InstanceID
group by
    base_with_account.accountid,
    base_with_account.InstanceID,
    base_with_account.licensetypeid,
    al.accountid,
    al.InstanceID,
    al.licensetypeid,
    BillingStartDate,
    PurchaseDate,
    PurchaseDate_month_trunc,
    license_acquitsion_dates,
    LicenseHops,
    LicenseType,
    license_count,
    license_acquitsion_quarter