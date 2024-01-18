import requests
from dotenv import load_dotenv
load_dotenv()
import os


#
fec_api = os.getenv('FEC_API')

fec_params = {
    "election_year": 2024,
    "sort": "-contribution_receipt_amount",
    "api_key": fec_api
}

def presidential_conts_by_state(**params):
    fec_url = f"https://api.open.fec.gov/v1/presidential/contributions/by_state/?page=1&per_page=100&election_year={params['election_year']}&sort=-contribution_receipt_amount&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

def candidates(**params): #Data for all candidates running for president
    fec_url = f"https://api.open.fec.gov/v1/candidates/?page=1&per_page=100&cycle={params['election_year']}&sort=name&office=P&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

def comittees(**params):
    fec_url = f"https://api.open.fec.gov/v1/committees/?page=1&per_page=100&cycle={params['election_year']}&committee_type=P&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key={params['api_key']}"
    fec_req = requests.get(fec_url)

    fec_json = fec_req.json()
    print(fec_json)

comittees(**fec_params)