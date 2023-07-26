import json
from Utilities import stats

data = json.load(open("data_test_tendencia.json"))

results = stats.indicador_cambio_tendencia(data["data_group1"], data["data_group2"])
print(results)