from llama_snowflake import SnowflakeDB

from llama_index import SQLDatabase

db = SnowflakeDB()

from llama_index.indices.struct_store import SQLTableRetrieverQueryEngine
from llama_index.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index import VectorStoreIndex

from llama_index.indices.struct_store import NLSQLTableQueryEngine,SQLTableRetrieverQueryEngine

sql_database = SQLDatabase(db._engine, include_tables=["mv_shipment"])

query_engine = NLSQLTableQueryEngine(sql_database)


response = query_engine.query("How many shipments picked up last week? The database is in Snowflake.")