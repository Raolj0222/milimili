import requests

headers = {
    'Authorization': 'Token token=EYPb4pZvh6pLPWL7y_Bk',
    'Content-Type': 'application/json',
}

json_data = {
    'parent_tracker': 'test',
    'name': '#145',
}

response = requests.post('https://api.adjust.com/public/v1/apps/3dle7bbjgwhs/trackers', headers=headers, json=json_data)

print(response.json())