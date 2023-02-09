SELECT DISTINCT g.COMPANY_ID,
                'Account Manager' as "Team Member Role",
                u.NAME            as "Team Member Name"
FROM ANALYTICS.DBO.GROWTH__OBT g
         INNER JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__ACCOUNT a ON g.COMPANY_ID = a.ID
         LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER u ON a.OWNER_ID = u.ID
UNION ALL
SELECT DISTINCT g.COMPANY_ID,
                atm.TEAM_MEMBER_ROLE,
                utm.NAME
FROM ANALYTICS.DBO.GROWTH__OBT g
         INNER JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__ACCOUNT a ON g.COMPANY_ID = a.ID
         LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__ACCOUNT_TEAM_MEMBER atm ON a.ID = atm.ACCOUNT_ID
    AND atm.IS_DELETED = false
         LEFT JOIN ANALYTICS.DBO_TRANSFORMATION.BASE_SALESFORCE__USER utm ON atm.USER_ID = utm.ID
WHERE atm.TEAM_MEMBER_ROLE NOT IN ('Account_Manager', 'Account Manager')