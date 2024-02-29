from src.services import predictor


def test_simple_prediction(historic_load_profile):
    simple_predictor = predictor.SimplePredictor()
    simple_predictor.configure(historic_load_profile, state='BB')
    prediction = simple_predictor.create_prediction()
    assert prediction is not None


def test_rfr_prediction(historic_load_profile):
    rfr_predictor = predictor.RandomForestPredictor()
    rfr_predictor.configure(historic_load_profile, state='BB')
    prediction = rfr_predictor.create_prediction()
    assert prediction is not None
