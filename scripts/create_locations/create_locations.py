import requests

from scripts.create_locations import teileinspeiser
from scripts.create_locations import vgp

run_on_prod = True

url_local = "http://127.0.0.1:8000/locations/"
url_prod = "https://ppapredictions.testsystem.node.energy/locations/"

api_key_local = "node"
api_key_prod = "WjrFmjVJ9Jx9NA9nRXFf8iVFcP"

if run_on_prod:
    url = url_prod
    api_key = api_key_prod
else:
    url = url_local
    api_key = api_key_local


template = {
    "state": "",
    "alias": "",
    "tso": None,
    "residual_short": {
        "number": ""
    },
    "residual_long": {
        "number": ""
    },
    "producers": [
        {
            "name": "",
            "market_location": {
                "number": ""
            },
            "prognosis_data_retriever": None
        },
    ],
    "settings": {
        "active_from": "",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": None
    }
}

location_payloads = [
    # teileinspeiser.riva,  # already migrated
    vgp.gerber3_g,
    vgp.gerber4_l,
    vgp.gerdus_5,
    vgp.gerdus_6,
    vgp.gerdus_7,
    vgp.gergoe_a,
    vgp.gerhal_abc,
    vgp.gerhal2_a,
    vgp.gerlaa_ph,
    vgp.germag_b,
    vgp.germag_f,
    vgp.gerobk_a,
    vgp.gerros_a2,
    vgp.gerwus_a1,
    vgp.gerwus_b1,
]


for location_payload in location_payloads:
    headers = {"Content-Type": "application/json", "X-API-KEY": api_key}
    response = requests.request("POST", url, json=location_payload, headers=headers)
    assert response.status_code == 200