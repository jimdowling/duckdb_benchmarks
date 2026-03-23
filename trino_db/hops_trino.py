import hopsworks
from hopsworks_common.core import project_api, secret_api
from hopsworks_common.core.variable_api import VariableApi
from trino.auth import BasicAuthentication
from trino.dbapi import connect as trino_connect

project = hopsworks.login()
fs = project.get_feature_store()

# Resolve Trino host and credentials (mirrors TrinoApi internals)
variable_api = VariableApi()
service_discovery_domain = variable_api.get_service_discovery_domain()
host = f"coordinator.trino.service.{service_discovery_domain}"

_project_api = project_api.ProjectApi()
username = _project_api.get_user_info()["username"]
user = f"{project.name}__{username}"

_secret_api = secret_api.SecretsApi()
password = _secret_api.get_secret(user).value

from hopsworks_common import client
ca_chain_path = client.get_instance()._get_ca_chain_path()

conn = trino_connect(
    host=host,
    port=8443,
    user=user,
    catalog="delta",
    schema=fs.name,
    auth=BasicAuthentication(user, password),
    http_scheme="https",
    verify=ca_chain_path,
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM serp_data_1 LIMIT 100")
rows = cursor.fetchall()
for row in rows:
    print(row)

