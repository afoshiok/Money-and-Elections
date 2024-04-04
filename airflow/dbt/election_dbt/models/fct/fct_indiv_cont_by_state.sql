with indiv_cont_source as (
    SELECT * FROM {{ ref('src_indiv_contributions') }}
)