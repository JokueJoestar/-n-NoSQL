import re
from cassandra.cluster import Cluster
from app.common import CASS_HOST, CASS_PORT

sql = open("schema.cql", encoding="utf8").read()
sql = re.sub(r"--.*", "", sql)
s = Cluster([CASS_HOST], port=CASS_PORT).connect()
for stmt in [x.strip() for x in re.split(r"\n\s*\n", sql) if x.strip()]:
    s.execute(stmt)
print("schema ok")
