with operating_exp_source as (
    SELECT * FROM {{ source('election', 'operating_exp') }}
),
report_typ_source as (
    SELECT * FROM {{ source('election', 'report_types') }}
)

SELECT
    oes.cmte_id,
    CASE
        WHEN oes.amndt_ind = 'N' THEN 'New'
        WHEN oes.amndt_ind = 'A' THEN 'Amendment'
        WHEN oes.amndt_ind = 'T' THEN 'Termination'
        WHEN oes.amndt_ind = NULL THEN oes.amndt_ind
        ELSE amndt_ind
    END as amndt_id,
    oes.rpt_yr,
    rt.rp_typ,
    oes.name,
    oes.city,
    oes.state,
    oes.zip_code,
    oes.transaction_dt,
    oes.purpose,
    oes.category_desc,
    oes.memo_text,
    oes.sub_id --Unique ID
FROM operating_exp_source oes
INNER JOIN report_typ_source rt ON oes.rpt_tp = rt.rp_typ_code