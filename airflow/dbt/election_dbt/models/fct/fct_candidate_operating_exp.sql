WITH operating_exp_source AS (
    SELECT * FROM {{ ref('src_operating_exp') }}
),
committee_source AS (
    SELECT * FROM {{ ref('src_committees') }}
),
cand_source AS (
    SELECT * FROM {{ ref('src_candidates') }}
)

SELECT
    oes.cmte_id,
    c.cmte_nm as cmte_name,
    cand.cand_name,
    oes.name,
    oes.city,
    oes.state,
    oes.purpose,
    oes.category_desc,
    oes.transaction_amt,
    oes.transaction_dt,
    oes.sub_id
FROM operating_exp_source oes
LEFT JOIN committee_source c ON oes.cmte_id = c.cmte_id
INNER JOIN cand_source cand ON cand.cand_id = c.cand_id --Only looking for expenditures made by Candidate Committees