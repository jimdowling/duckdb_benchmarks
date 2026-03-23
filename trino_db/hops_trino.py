import hopsworks

project = hopsworks.login()
trino_api = project.get_trino_api()
conn = trino_api.connect(catalog="delta", schema="jim_featurestore") 
cursor = conn.cursor()
cursor.execute("SELECT * FROM transactions LIMIT 100")
rows = cursor.fetchall()
for row in rows:
    print(row)

