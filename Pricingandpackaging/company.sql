select distinct COMPANY_ID, COMPANY_NAME,COMPANY_NAME_WITH_ID
from ANALYTICS.DBO.GROWTH__OBT
                           where      1 = 1
                                AND company_name != ''
                                AND metric_object = 'applied_billings'