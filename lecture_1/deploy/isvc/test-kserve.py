import requests
import json

api_host = 'category-match.default.svc.cluster.local'
model_name = 'category-match'

url = f'http://{api_host}/v1/models/{model_name}:predict'

headers = {'Content-Type': 'application/json'}
payload = {"instances": [[0]*20]}

res = requests.post(url, headers=headers, data=json.dumps(payload)).json()