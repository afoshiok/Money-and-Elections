.
├── analyses
├── macros
├── models/
│   ├── fct/
│   │   ├── fct_cand_committee_trans.sql -- Transactions from committes to candidates
│   │   ├── fct_candidate_indiv_cont.sql -- Totals of individual contributions by candidates
│   │   ├── fct_candidate_operating_exp.sql -- All operating expenditures made by candidate-authorized committees
│   │   ├── fct_categories_operating_exp.sql -- Totals of operating expenditures grouped by categories
│   │   ├── fct_committee_operating_exp.sql -- All operating expenditures by committees
│   │   └── fct_indiv_cont_by_state.sql -- Totals of individual contributions grouped by states and territories
│   └── src/
│       ├── src_candidates.sql
│       ├── src_committee_transactions.sql
│       ├── src_committees.sql
│       ├── src_independent_exp.sql
│       ├── src_indiv_contributions.sql
│       └── src_operating_exp.sql
├── seeds
├── snapshots
└── tests
