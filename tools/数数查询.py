import pandas as pd
import re
import requests
import datetime
def get_sqldata(token,sql):
    url = "http://13.214.112.1:8992/querySql"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    params = {
        "token": token,
        'sql': f"""{sql}""",
        "format": "json",
        "timeoutSeconds": "600"
    }
    response = requests.post(url, headers=headers, params=params)
    response = re.split('[\n]', response.text)
    df_list = []
    for i in response[1:]:
        lis = re.sub('[\r@"\[\]]', '', i)
        df_list.append(re.split(',', lis))
    df = pd.DataFrame(df_list)
    return df
