import json

import pandas as pd
data = pd.DataFrame([[1,2,3],[1,3,4]])
data.columns=['a','b','c']
data_dict=data.to_json(orient='records')
values=json.loads(data_dict)
# # values = ', '.join(['%s'] * len(data_dict))
# print(tuple(data_dict.values()))
print(values)
