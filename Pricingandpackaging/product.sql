select PRODUCT_CATEGORIZATION_ARR_REPORTED_PRODUCT,
       PRODUCT_CATEGORIZATION_PRODUCT_PORTFOLIO,
       PRODUCT_CATEGORIZATION_PRODUCT_GROUP,
       PRODUCT_CATEGORIZATION_PRODUCT_LINE,
       PRODUCT_CATEGORIZATION_PRODUCT_PACKAGE,
       PRODUCT_CATEGORIZATION_PRODUCT_PLAN,
       PRODUCT_CATEGORIZATION_PRODUCT_VENDOR,
       PRODUCT_CATEGORIZATION_ACCOUNT_GROUP,
       PRODUCT_CATEGORIZATION_LICENSE_SERVICE_TYPE
from ANALYTICS.DBO.GROWTH__OBT
where PRODUCT_CATEGORIZATION_PRODUCT_VENDOR <> '?? nothing to see here'
  and PRODUCT_CATEGORIZATION_PRODUCT_PLAN <> '?? nothing to see here'
  and PRODUCT_CATEGORIZATION_ACCOUNT_GROUP <> '?? nothing to see here'