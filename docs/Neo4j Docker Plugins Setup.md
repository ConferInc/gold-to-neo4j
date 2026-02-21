Docker‑based Neo4j Plugins (GDS + GenAI) — Detailed Steps
What "Docker host" means
The Docker host is the server/VM where the Neo4j Docker container is running.
All file paths like /opt/neo4j/... live on that host, not inside the container.

1) Create host folders
On the Docker host:

sudo mkdir -p /opt/neo4j/{conf,plugins,data,logs,import}
This is where config, plugin jars, and data will persist.

2) Edit neo4j.conf
File location (on the host):

/opt/neo4j/conf/neo4j.conf
Add these lines (or update if they exist):

dbms.security.procedures.unrestricted=gds.*,genai.*
dbms.security.procedures.allowlist=gds.*,genai.*
If the file doesn't exist yet, create it:

sudo touch /opt/neo4j/conf/neo4j.conf
sudo nano /opt/neo4j/conf/neo4j.conf
3) Create genai.conf
File location (on the host):

/opt/neo4j/conf/genai.conf
Create it:

sudo touch /opt/neo4j/conf/genai.conf
sudo nano /opt/neo4j/conf/genai.conf
Example content (replace with your LiteLLM endpoint + API key):

genai.openai.baseurl=https://<your-litellm-host>/v1
genai.openai.apikey=<YOUR_PROXY_API_KEY>
4) Run Neo4j container with plugins enabled
This uses the Neo4j server image tag matching your kernel version:

docker run -d \
  --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/your-password \
  -e NEO4J_PLUGINS='["graph-data-science","genai"]' \
  -v /opt/neo4j/conf:/conf \
  -v /opt/neo4j/plugins:/plugins \
  -v /opt/neo4j/data:/data \
  -v /opt/neo4j/logs:/logs \
  -v /opt/neo4j/import:/import \
  neo4j:2026.01.4
What this does:

NEO4J_PLUGINS auto‑downloads GDS + GenAI jars.
/conf is where neo4j.conf and genai.conf are read from.
/plugins is where jars are stored.
/data and /logs persist Neo4j data/logs.
5) If container already exists
If Neo4j is already running, you should stop and recreate the container with the new settings (recommended), or:

docker restart neo4j
6) Verify in Neo4j Browser
CALL dbms.procedures() YIELD name
WHERE name STARTS WITH 'gds.' OR name STARTS WITH 'genai.'
RETURN name LIMIT 20;
Notes
The GenAI keys (genai.openai.baseurl, genai.openai.apikey) must match the Neo4j GenAI plugin docs.
If LiteLLM requires a different header or key name, your admin should set it accordingly in genai.conf.
