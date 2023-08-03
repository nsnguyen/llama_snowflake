import sqlalchemy
import logging

from sqlalchemy.exc import ProgrammingError

LOGGER = logging.getLogger(__name__)


def _dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


class BaseConnection(object):
  def __init__(self) -> None:
      self._engine = None
      self._schema = None
      
  def _init_database(self, db_type, dialect, user, pwd, host, port):
    url = db_type + '+' + dialect + '://' + user + ':' + pwd + '@' + host + ':' + str(port) + '/' + self.db_name
    self._create_engine(url)
        
  def _create_engine(self, url, **kwargs):
      kwargs['pool_pre_ping'] = True
      self._engine = sqlalchemy.create_engine(url, **kwargs)
      self._engine.connect()
        
  def _init_database_with_url(self, url, connect_args={}):
    self._create_engine(url, connect_args=connect_args)
    
  def _connect(self):
    return self._engine.connect()
        
  def fetchall(self, sql, params=()):
    conn = self._connect()
    conn.row_factory = _dict_factory
    query = conn.execute(sql, params)
    results = query.fetchall()
    results = [dict(u) for u in results]
    return results