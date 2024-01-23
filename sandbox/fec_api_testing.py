import requests
from dotenv import load_dotenv
load_dotenv()
import os


#
fec_api = os.getenv('FEC_API')

fec_params = { #Test Params
    "election_year": 2024,
    "sort": "-contribution_receipt_amount",
    "office": "P", #P = President, H = House, S = Senate
    "api_key": fec_api
}

def presidential_conts_by_state(**params):
    fec_url = f"https://api.open.fec.gov/v1/presidential/contributions/by_state/?page=1&per_page=100&election_year={params['election_year']}&sort=-contribution_receipt_amount&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

def candidates(**params):
    fec_url = f"https://api.open.fec.gov/v1/candidates/?page=1&per_page=100&cycle={params['election_year']}&sort=name&office={params['office']}&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

def comittees(**params):
    fec_url = f"https://api.open.fec.gov/v1/committees/?page=1&per_page=100&cycle={params['election_year']}&committee_type=P&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

def schedule_a(**params): #Schedule A records describe itemized receipts, including contributions from individuals. Not paginated by number, must append the given "last index" to endpoint until = null.
    first_schedule_a_url = f"https://api.open.fec.gov/v1/schedules/schedule_a/?two_year_transaction_period=2024&per_page=100&sort=-contribution_receipt_date&sort_hide_null=false&sort_null_only=false&api_key={params['api_key']}"
    last_index = {
        "last_contribution_receipt_date" : None,
        "last_index": None
    }
    result = []
    first_schedule_a_req = requests.get(first_schedule_a_url)
    first_res = first_schedule_a_req.json()
    new_index = first_res["pagination"]["last_indexes"]
    last_index["last_contribution_receipt_date"] = new_index.get("last_contribution_receipt_date", None)
    last_index["last_index"] = new_index["last_index"]
    while last_index["last_contribution_receipt_date"] is not None:
        rec_url = f"https://api.open.fec.gov/v1/schedules/schedule_a/?last_contribution_receipt_date={new_index['last_contribution_receipt_date']}&two_year_transaction_period=2024&per_page=100&last_index={new_index['last_index']}&sort=-contribution_receipt_date&sort_hide_null=false&sort_null_only=false&api_key={params['api_key']}"
        page_req = requests.get(rec_url)
        page_res = page_req.json()
        page_index = page_res["pagination"]["last_indexes"]
        last_index["last_contribution_receipt_date"] = page_index.get("last_contribution_receipt_date", None)
        last_index["last_index"] = page_index["last_index"]
        result.extend(page_res["results"])
    print(len(result))



# def schedule_b(**params): #This data explains how committees and other filers spend their money.

# def schedule_c(**params): #Schedule C shows all loans, endorsements and loan guarantees a committee receives or makes.

# def schedule_d(**params): #Schedule D, it shows debts and obligations owed to or by the committee that are required to be disclosed.

# comittees(**fec_params)