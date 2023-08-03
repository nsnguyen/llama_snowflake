import os

import snowflake.connector

from snowflake.sqlalchemy import URL

from llama_snowflake.base_connection import BaseConnection
from dotenv_vault import load_dotenv
load_dotenv()

class SnowflakeDB(BaseConnection):
  def __init__(self,
               account=None,
               user=None,
               password=None,
               schema=None,
               warehouse=None,
               database=None,
               role=None) -> None:
    super().__init__()
    self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    self.user = user or os.getenv("SNOWFLAKE_USER")
    self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
    self.schema = schema or 'PUBLIC'
    self.warehouse = warehouse or 'AD_HOC'
    self.database = database or 'DEV'
    self.role = role or 'DEV'
    
    self.conn = snowflake.connector.connect(
      user=self.user,
      password=self.password,
      account=self.account,
      warehouse=self.warehouse,
      database=self.database,
      schema=self.schema
      )
    # dict of arguments supplied in the URL
    url_args = {
      'user': self.user,
      'password': self.password,
      'account': self.account,
      'database': self.database,
      'warehouse': self.warehouse,
      'schema': self.schema,
      'role': self.role
    }
    
    # dict of arguments supplied as connect_args
    connect_args = {
      'client_session_keep_alive': True
    }
    url = URL(**url_args)
    self._init_database_with_url(url=url, connect_args=connect_args)
