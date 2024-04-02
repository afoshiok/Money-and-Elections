with ct_source as (
    SELECT * FROM {{ source('election', 'committee_trans') }}
),
report_typ_source as (
    SELECT * FROM {{ source('election', 'report_types') }}
),
transaction_typ_source as (
    SELECT * FROM {{ source('election', 'transaction_types') }}
)

SELECT
    ct.cmte_id,
    CASE
        WHEN ct.amndt_ind = 'N' THEN 'New'
        WHEN ct.amndt_ind = 'A' THEN 'Amendment'
        WHEN ct.amndt_ind = 'T' THEN 'Termination'
        WHEN ct.amndt_ind = NULL THEN ct.amndt_ind
        ELSE amndt_ind
    END as amndt_id,
    rt.rp_typ,
    ct.transaction_pgi,
    tt.trans_tp_desc,
    ct.entity_tp,
    ct.name,
    ct.city,
    ct.state,
    ct.zip_code,
    ct.employer,
    ct.occupation,
    ct.transaction_dt,
    ct.transaction_amt,
    ct.other_id,
    ct.memo_text,
    ct.sub_id --Unique ID
FROM ct_source ct
INNER JOIN report_typ_source rt ON ct.rpt_tp = rt.rp_typ_code
INNER JOIN transaction_typ_source tt ON ct.transaction_tp = tt.trans_tp