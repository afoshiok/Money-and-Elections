WITH independent_exp_source AS (
    SELECT * FROM {{ ref('src_independent_exp') }}
    WHERE CAND_ID IS NOT NULL
),

cand_source AS (
    SELECT * FROM {{ ref('src_candidates') }}
),

committee_source AS (
    SELECT * FROM {{ ref('src_committees') }}
)

SELECT
    ies.cmte_id,
    cmte.cmte_nm,
    ies.amndt_id,
    ies.rp_typ,
    ies.trans_tp_desc,
    cs.cand_name as cand_repcipient,
    ies.city as cand_city,
    ies.state as cand_state,
    ies.zip_code as cand_zip_code,
    ies.transaction_dt,
    ies.transaction_amt,
    ies.sub_id
FROM independent_exp_source ies
INNER JOIN cand_source cs ON ies.cand_id = cs.cand_id
INNER JOIN committee_source cmte ON ies.cmte_id = cmte.cmte_id
WHERE amndt_id != 'Termination'