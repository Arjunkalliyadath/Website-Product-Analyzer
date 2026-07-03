import requests
r = requests.post('http://127.0.0.1:8000/analyze', data={'company_name':'Starbucks kozhikode'}, timeout=60)
print(r.status_code)
print(r.headers.get('content-type'))
print(r.text[:800])
