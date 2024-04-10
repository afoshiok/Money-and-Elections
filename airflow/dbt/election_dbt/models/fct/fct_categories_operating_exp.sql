WITH operating_exp_source AS (
    SELECT * FROM {{ ref('fct_committee_operating_exp') }}
)
SELECT
    CASE
        WHEN CATEGORY_DESC IS NULL THEN 'Other'
        ELSE CATEGORY_DESC
    END as CATEGORY,
    SUM(TRANSACTION_AMT) as transaction_amt
FROM operating_exp_source
GROUP BY CATEGORY