![alt tag](http://i.imgur.com/UPNp8lR.jpg?3)
# Gonz #

*A command line utility to translate Postgres views and BigQuery tables into Neo4j nodes and relationships.*

### Setup: ###

*Gonz depends on the [apoc extension for neo4j](https://github.com/neo4j-contrib/neo4j-apoc-procedures), place the jar in your neo4j plugins folder, along with the [jdbc connector for postgres](https://jdbc.postgresql.org/download.html) and the [jdbc connector for BigQuery](https://cloud.google.com/bigquery/partners/simba-beta-drivers). If you plan to use BigQuery you will also need a service account JSON Web Token or a OAuthCLientSecret from google which has access to BQ.*

### Usage: ###

*Install the dependancies with -g option:*
```bash
npm install -g
```

*Modify config.yml to conform to your local development environment:*

```yml
postgres:
  host: 127.0.0.1
  port: 5432
  user: postgres
  password: 
  database: my_pg_db
  idleTimeoutMillis: 1000
bigquery:
  host: https://www.googleapis.com/bigquery/v2
  port: 443
  OAuthPvtKeyPath: /Users/path/to_key/jsonwebtoken-7c2c04303bb3.json
  OAuthServiceAcctEmail: email_from@token.iam.gserviceaccount.com
  OAuthType: 0
  Timeout: 60
  ProjectId: yourproject
db:
  schema: graph_import
  node_prefix: node_
  rel_prefix: relationship_
  page: 1000000
import:
  batch_size: 5000
  parallel: true
neo4j:
  path: /Users/dudeman/neo4j-enterprise-3.0.3
   
```

*Create views in Postgres or tables in BQ which conform to the settings in your config.yml:*

```sql
--- Node View
CREATE MATERIALIZED VIEW graph_import.node_taxonomy AS 
SELECT 
  global_id as id, 
  classification, 
  specialization
FROM taxonomies;

--- Relationship View
CREATE MATERIALIZED VIEW graph_import.relationship_attended_school as 
SELECT
    pid as provider_id, 
    schid as school_id, 
    med_school_graduation_year as graduation_year 
FROM education; 
```

*Create cypher statements for and import all data from the views defined in the schema you defined:*

```bash
gonz pg:all import=true
```

*Create cypher statements for all rels in the BQ schema without importing:*

```bash
gonz bq:rels
```

*Create statements for and import a specific Node:*

```bash
gonz pg:node[Provider] import=true
```

*Create statements for and import a specific Relationship:*

```bash
gonz pg:rel[HAS_OWNERSHIP_IN] import=true
```

*Create all indexes for the UID on all nodes in your BQ schema*

```bash
gonz bq:index import=true
```

### Conventions: ###

* All pg data must be in regular views or materialized views. 
* All bq tables gotta be tables and not views (for now - JDBC limitation)
* All nodes must use prefix defined in config.yml *(default: node_)*
* All rels must use prefix defined in config.yml *(default: relationship_)*
* The first column of a node view/table is always UID, all subsequent columns are properties on the node. 
* Relationship views/tables define the first column as the UID of the from node and the second column as the UID of the to node, all subsequent columns are properties on the relationship.
* The from and to columns should follow the format from_node_type_id, to_node_type_id, eg *(provider_id, county_id) -- if it is from the same type to the same type, provide a distinction after _id - (provider_id_from, provider_id_to)*
* All Date properties should be converted to unix timestamp with: *EXTRACT(EPOCH FROM date_field)*
* All JSON properties should be either ignored or converted to Strings.
* Singular Naming for Nodes and Relationships.
* snake_case in Postgres and BQ result in PascalCase in Neo4j
