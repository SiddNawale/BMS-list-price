select distinct iwal.instanceid,
                iwal.accountid,
                OrganizationName,
                PrimaryContactFirstName,
                PrimaryContactLastName,
                PrimaryContactEmailAddress,
                iwal.LicenseTypeID,
                iwal.licensecount,
                iwal.billingintervalmonthcount,
                iwal.billingstartdate,
                ac.CreationTime     as                                                         account_creation,
                max(ic.ResolutionTime) as                                                         LicensePurchaseTime,
                webhostname,
                ac.CurrencyCode,
                ISNULL(ISNULL(LP.Price, 0.00) / NULLIF(ic.billingintervalmonthcount, 0), 0.00) MRR
from Reporting.InstancesWithActiveLicense iwal
         left join Account ac
                   on ac.AccountID = iwal.AccountID
         left join InstanceChange ic
                   on ic.InstanceID = iwal.InstanceID
         LEFT JOIN LicensePrice LP
                   ON iwal.LicenseTypeID = LP.LicenseTypeID
                       AND iwal.LicenseCount = LP.LicenseCount
                        AND iwal.BillingIntervalMonthCount = LP.BillingIntervalMonthCount
                        AND Ac.CurrencyCode = LP.CurrencyCode

where
   Resolution = 1
and InstanceChangeID in (
                  SELECT MAX(InstanceChangeID)
                  FROM InstanceChange
                  WHERE Resolution = 1
                  GROUP BY InstanceID
              )
group by
iwal.instanceid,
                iwal.accountid,
                OrganizationName,
                PrimaryContactFirstName,
                PrimaryContactLastName,
                PrimaryContactEmailAddress,
                iwal.LicenseTypeID,
                iwal.licensecount,
                iwal.billingintervalmonthcount,
                iwal.billingstartdate,
                ac.CreationTime,
                webhostname,
                ac.CurrencyCode,
                Price,
                ic.billingintervalmonthcount