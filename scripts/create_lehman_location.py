import requests

from src.prognosis.enums import DataRetriever, TransmissionSystemOperator

url = "http://127.0.0.1:8000/locations/"
headers = {"Content-Type": "application/json", "X-API-KEY": "node"}


locations = [
    {
        "id": "5a759f56-322b-4d4f-abae-d3c22318ec3f",
        "state": "NW",
        "alias": "00000000042_Teileinspeisung_Lehmann",
        "tso": TransmissionSystemOperator.TENNET.value,
        "residual_short": {
            # "number": "50184333926"
            "number": "DE0010873242900000000000000005499"   # currently we only get data for this melo
        },
        "residual_long": {
            "id": "fde5bb7e-b0a9-4dc5-ae9e-9ca109777cb7",
            "number": "50186896518"
        },
        "producers": [
            {
                "id": "21b06b12-2f7a-4913-bc4f-c0dbf677d8bd",
                "name": "PV_Lehmann",
                "market_location": {
                    # "id": "???",
                    "number": "DE0010873242900000000000000113459"
                },
                "prognosis_data_retriever": DataRetriever.IMPULS_ENERGY_TRADING_SFTP.value
            },
        ],
        "settings": {
            "active_from": "2024-01-01",
            "active_until": None,
        }
    }
]


for location in locations:
    payload = {
        "id": location.get("id"),
        "state": location.get("state"),
        "alias": location.get("alias"),
        "tso": location.get("tso"),
        "residual_short": {
            "id": location.get("residual_short").get("id"),
            "number": location.get("residual_short").get("number"),
        },
        "residual_long": {
            "id": location.get("residual_long").get("id"),
            "number": location.get("residual_long").get("number"),
        },
        "producers": [
            {
                "id": producer.get("id"),
                "name": producer.get("name"),
                "market_location": {
                    "id": producer.get("market_location").get("id"),
                    "number": producer.get("market_location").get("number"),
                },
                "prognosis_data_retriever": producer.get("prognosis_data_retriever"),
            }
            for producer in location.get("producers")
        ],
        "settings": {
            "active_from": location.get("settings").get("active_from"),
            "active_until": location.get("settings").get("active_until")
        },
    }
    response = requests.request("POST", url, json=payload, headers=headers)
    assert response.status_code == 200