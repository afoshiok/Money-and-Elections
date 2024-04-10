WITH operating_exp_source AS (
    SELECT * FROM {{ ref('src_operating_exp') }}
),
committee_source AS (
    SELECT * FROM {{ ref('src_committees') }}
)

SELECT
    oes.cmte_id,
    c.cmte_nm as cmte_name,
    oes.name,
    oes.city,
    oes.state,
    oes.purpose,
    oes.category_desc,
    oes.transaction_amt,
    oes.transaction_dt,
    oes.sub_id
FROM operating_exp_source oes
INNER JOIN committee_source c ON oes.cmte_id = c.cmte_id