import requests

from src.enums import DataRetriever, TransmissionSystemOperator

run_on_prod = False

url_local = "http://127.0.0.1:8000/locations/"
url_prod = "https://ppapredictions.testsystem.node.energy/locations/"

api_key_local = "node"
api_key_prod = None

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

lehmann = {
    "id": "5a759f56-322b-4d4f-abae-d3c22318ec3f",
    "state": "NW",
    "alias": "Teileinspeisung Lehmann",
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
                "number": "DE0010873242900000000000000113459"
            },
            "prognosis_data_retriever": DataRetriever.IMPULS_ENERGY_TRADING_SFTP.value
        },
    ],
    "settings": {
        "active_from": "2024-01-01",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": False
    }
}

riva = {
    "state": "BW",
    "alias": "Teileinspeisung PV Riva",
    "tso": TransmissionSystemOperator.AMPRION.value,
    "residual_short": {
        "number": "50280364817"
    },
    "residual_long": {
        "number": "51662700463"
    },
    "producers": [
        {
            "name": "PV MvAA Backnang (1)",
            "market_location": {
                "number": "51662699682"
            },
            "prognosis_data_retriever": DataRetriever.ENERCAST_SFTP.value
        },
        {
            "name": "PV MvAA Backnang (2)",
            "market_location": {
                "number": "51662699674"
            },
            "prognosis_data_retriever": DataRetriever.ENERCAST_SFTP.value
        },
    ],
    "settings": {
        "active_from": "2024-04-01",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": False
    }
}

geis_ebersdorf = {
    "state": "BY",
    "alias": "Geis - 51553306479_PV_Geis",
    "tso": TransmissionSystemOperator.TENNET.value,
    "residual_short": {
        "number": "51553306510"
    },
    "residual_long": {
        "number": "51553306479"
    },
    "producers": [
        {
            "name": "PV-Anlage Ebersdorf",
            "market_location": {
                "number": "51553306677"
            },
            "prognosis_data_retriever": DataRetriever.ENERCAST_SFTP.value
        },
    ],
    "settings": {
        "active_from": "2024-05-01",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": False
    }
}

geis_bad_neustadt = {
    "state": "BY",
    "alias": "Geis - Teileinspeisung Bad Neustadt",
    "tso": TransmissionSystemOperator.TENNET.value,
    "residual_short": {
        "number": "51217262355"
    },
    "residual_long": {
        "number": "51217386395"
    },
    "producers": [
        {
            "name": "PV-Anlage Bad Neustadt",
            "market_location": {
                "number": "DE00041797616000X10653024YH04X002"
            },
            "prognosis_data_retriever": DataRetriever.ENERCAST_SFTP.value
        },
    ],
    "settings": {
        "active_from": "2024-09-01",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": False
    }
}

wetzel_daxlanden = {
    "state": "BW",
    "alias": "Schrott Wetzel West - Daxlanden",
    "tso": TransmissionSystemOperator.TRANSNET.value,
    "residual_short": {
        "number": "50784049147"
    },
    "residual_long": {
        "number": "50784697805"
    },
    "producers": [
        {
            "name": "Daxlanden 125er (Nordbeckenstraße)",
            "market_location": {
                "number": ""    # todo we have no metering data for this consumer. Use predicted prognosis instead. Not implemented yet!
            },
            "prognosis_data_retriever": DataRetriever.ENERCAST_SFTP.value
        },
    ],
    "settings": {
        "active_from": "2024-01-01",
        "active_until": None,
        "send_consumption_predictions_to_fahrplanmanagement": True
    }
}

location_payloads = [
    riva
]


for location_payload in location_payloads:
    headers = {"Content-Type": "application/json", "X-API-KEY": api_key}
    response = requests.request("POST", url, json=location_payload, headers=headers)
    assert response.status_code == 200