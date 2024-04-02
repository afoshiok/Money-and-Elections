with ic_source as (
    SELECT * FROM {{ source('election', 'indiv_contributions') }}
),
report_typ_source as (
    SELECT * FROM {{ source('election', 'report_types') }}
),
transaction_typ_source as (
    SELECT * FROM {{ source('election', 'transaction_types') }}
)

SELECT
    ic.cmte_id,
    CASE
        WHEN ic.amndt_ind = 'N' THEN 'New'
        WHEN ic.amndt_ind = 'A' THEN 'Amendment'
        WHEN ic.amndt_ind = 'T' THEN 'Termination'
        WHEN ic.amndt_ind = NULL THEN ic.amndt_ind
        ELSE amndt_ind
    END as amndt_id,
    rt.rp_typ,
    ic.transaction_pgi,
    tt.trans_tp_desc,
    ic.name,
    ic.city,
    ic.zip_code,
    ic.employer,
    ic.occupation,
    ic.transaction_dt,
    ic.transaction_amt,
    ic.tran_id,
    ic.sub_id
FROM ic_source ic
INNER JOIN report_typ_source rt ON ic.rpt_tp = rt.rp_typ_code
INNER JOIN transaction_typ_source tt ON ic.transaction_tp = tt.trans_tp