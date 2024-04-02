with independent_exp_source as (
    SELECT * FROM {{ source('election', 'independent_exp') }}
),
report_typ_source as (
    SELECT * FROM {{ source('election', 'report_types') }}
),
transaction_typ_source as (
    SELECT * FROM {{ source('election', 'transaction_types') }}
)

SELECT
    cmte_id,
    CASE
        WHEN ies.amndt_ind = 'N' THEN 'New'
        WHEN ies.amndt_ind = 'A' THEN 'Amendment'
        WHEN ies.amndt_ind = 'T' THEN 'Termination'
        WHEN ies.amndt_ind = NULL THEN ies.amndt_ind
        ELSE amndt_ind
    END as amndt_id,
    rt.rp_typ,
    ies.transaction_pgi,
    tt.trans_tp_desc,
    ies.entity_tp,
    ies.name,
    ies.city,
    ies.state,
    ies.zip_code,
    ies.employer,
    ies.occupation,
    ies.transaction_dt,
    ies.transaction_amt,
    ies.other_id,
    ies.cand_id,
    ies.sub_id --Unique ID
FROM independent_exp_source ies
INNER JOIN report_typ_source rt ON ies.rpt_tp = rt.rp_typ_code
INNER JOIN transaction_typ_source tt ON ies.transaction_tp = tt.trans_tp