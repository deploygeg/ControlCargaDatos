import requests
import json

#parametros de los datos enviados
params = {"valores":[1,5,6,3,8,5,6,4,9,6,5,2,4,2,5]}
url='http://localhost/promedio'
headers = {'Content-Type':'application/json','Accept':'text/plain'}
response = requests.post(url,data=json.dumps(params),headers=headers)
print(response.status_code)
print(response.json())