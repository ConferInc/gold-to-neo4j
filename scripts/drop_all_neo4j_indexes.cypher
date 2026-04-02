// Drop ALL indexes (including vector) - single command
// Requires APOC plugin. Copy and paste into Neo4j Browser.

CALL db.indexes() YIELD name
WHERE name IS NOT NULL
CALL apoc.cypher.run('DROP INDEX `' + name + '` IF EXISTS', {})
YIELD value
RETURN name
