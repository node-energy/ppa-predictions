import requests


locations_consumers = [
    {"malo_short": "10026184572", "state": "NI", "alias": "GERLAA-PHW"},
    {"malo_short": "50420256824", "state": "ST", "alias": "GERMAG-B"},
    {"malo_short": "50121825176", "state": "SN", "alias": "GERLEI_C1_C2"},
    {"malo_short": "10017293283", "state": "SN", "alias": "GERLFH_A"},
    {"malo_short": "51310347476", "state": "HE", "alias": "GERGIN-A1_und_5"},
    {"malo_short": "50956270132", "state": "MV", "alias": "GERROS_A1"},
    {"malo_short": "50957653254", "state": "MV", "alias": "GERROS_A2"},
    {"malo_short": "50957679028", "state": "MV", "alias": "GERROS_A3"},
    {"malo_short": "50352654328", "state": "NI", "alias": "GERHAM-A3"},
    {"malo_short": "51559038787", "state": "HE", "alias": "GERWET-AB"},
    {"malo_short": "50862356117", "state": "NI", "alias": "GERGOE-A"},
    {"malo_short": "10026043710", "state": "NI", "alias": "GERLAA-PH"},
    {"malo_short": "50482109194", "state": "BB", "alias": "GERWUS-B1.3"},
    {"malo_short": "50482438072", "state": "BB", "alias": "GERWUS-A1"},
    {"malo_short": "50481892146", "state": "BB", "alias": "GERWUS-B1"},
    {"malo_short": "50483314213", "state": "BB", "alias": "GERWUS-C1-C2"},
    {"malo_short": "50478332486", "state": "BB", "alias": "GERBER-A"},
    {"malo_short": "51211092534", "state": "ST", "alias": "GERHAL2"},
    {"malo_short": "50121612896", "state": "ST", "alias": "GERHAL-ABC"},
    {"malo_short": "50483670722", "state": "BB", "alias": "GERBER2-C"},
    {"malo_short": "50482611941", "state": "BB", "alias": "GEROBK-A"},
    {"malo_short": "51656475535", "state": "BB", "alias": "GEROBK-E"},
    {"malo_short": "50456628881", "state": "HE", "alias": "GERBUS-A2"},
    {"malo_short": "50456628831", "state": "HE", "alias": "GERBUS-A"},
    {"malo_short": "50456628899", "state": "HE", "alias": "GERBUS-A3"},
    {"malo_short": "50456628873", "state": "HE", "alias": "GERBUS-A1"},
    {"malo_short": "50862432602", "state": "NI", "alias": "GERGOE-E"},
    {"malo_short": "50352653338", "state": "NI", "alias": "GERHAM2-B2+3"},
    {"malo_short": "50352648694", "state": "NI", "alias": "GERHAM-A1"},
    {"malo_short": "50481996534", "state": "BB", "alias": "GERBER3-G"},
    {"malo_short": "50303825639", "state": "BY", "alias": "GERMUE-PHN"},
    {"malo_short": "50995822689", "state": "HE", "alias": "GERROD-D"},
    {"malo_short": "50995829552", "state": "HE", "alias": "GERROD-A"},
    {"malo_short": "50419857112", "state": "ST", "alias": "GERMAG-B"},
    {"malo_short": "50420179414", "state": "ST", "alias": "GERMAG-F"},
    {"malo_short": "50420461639", "state": "ST", "alias": "GERMAG-C"},
    {"malo_short": "10057257348", "state": "TH", "alias": "GERERF3_A"},
]

url = "http://localhost:8002/locations/"
headers = {"Content-Type": "application/json", "X-API-KEY": "node"}


for location in locations_consumers:
    payload = {
        "residual_short": {"malo": location.get("malo_short")},
        "alias": location.get("alias"),
        "state": location.get("state"),
    }
    response = requests.request("POST", url, json=payload, headers=headers)
    assert response.status_code == 200
