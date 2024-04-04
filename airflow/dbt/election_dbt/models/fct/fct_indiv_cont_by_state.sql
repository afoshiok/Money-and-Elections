with indiv_cont_source as (
    SELECT * FROM {{ ref('src_indiv_contributions') }}
)

SELECT
    state,
    SUM(transaction_amt) AS sum_individual_cont
FROM indiv_cont_source
GROUP BY state