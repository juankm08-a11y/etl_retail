import pandas as pd  

df = pd.read_excel("data/OnlineRetail.xlsx",sheet_name="Online Retail")

df.to_csv("data/OnlineRetail.csv",index=False,encoding='utf-8')