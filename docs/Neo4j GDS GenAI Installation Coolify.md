Neo4j GDS + GenAI Installation (Coolify)
1) Update Environment Variables (Coolify UI)
Add to the existing Neo4j service:

NEO4J_PLUGINS=["graph-data-science","genai"]

NEO4J_dbms_security_procedures_allowlist=gds.*,genai.*
NEO4J_dbms_security_procedures_unrestricted=gds.*,genai.*

LITELLM_BASE_URL=https://<your-litellm-host>/v1
LITELLM_API_KEY=<your_proxy_key>
2) Add genai.conf in Coolify
Create or mount a config file at genai.conf with:

genai.openai.baseurl=$(printenv LITELLM_BASE_URL)
genai.openai.apikey=$(printenv LITELLM_API_KEY)
This keeps secrets in env vars only.

3) Add startup args
In Coolify "Command / Args", add:

--expand-commands
(Required to resolve $(printenv …) in genai.conf.)

4) Redeploy the Neo4j service
Redeploy/restart the existing Neo4j service in Coolify so the plugins are installed and configs applied.

5) Verify in Neo4j Browser
CALL dbms.procedures() YIELD name
WHERE name STARTS WITH 'gds.' OR name STARTS WITH 'genai.'
RETURN name LIMIT 20;

6) Post-redeploy validation checklist
1. Procedures visible (run step 5 query above and confirm results)
2. Small embed test
CALL genai.vector.encode('hello world') YIELD embedding
RETURN size(embedding) AS dims;
3. Small structural embedding test (GDS)
CALL gds.graph.project('__struct_test', 'Recipe', 'USES_INGREDIENT');
CALL gds.fastRP.stream('__struct_test', {embeddingDimension: 16, iterationWeights: [0.8, 1.0]})
YIELD nodeId, embedding
RETURN size(embedding) AS dims
LIMIT 1;
CALL gds.graph.drop('__struct_test');
