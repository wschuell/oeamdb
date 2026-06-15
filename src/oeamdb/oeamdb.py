
import sqlite3


from sqlalchemy import (
    create_engine,
    text,
)
from .sql_model import Base





def chunked(iterable, size):
    it = iter(iterable)
    while (batch := list(islice(it, size))):
        yield batch


class Importer:
    def __init__(self,query,chunk_size=10**4,param_processor=None,autocommit=True,param_split=False):
        self.chunk_size = chunk_size
        self.query = query
        self.param_processor = param_processor
        self.param_split = param_split
        self.autocommit = autocommit

    def import_all(self,engine,params):
        with engine.connect() as conn:
            param_iter = iter(params)
            while (chunk := list(islice(param_iter, self.chunk_size))):
                if self.param_processor is not None:
                    if self.param_split:
                        chunk_tmp = []
                        for p in chunk:
                            chunk_tmp += self.param_processor(p)
                        chunk = chunk_tmp
                    else:
                        chunk = [self.param_processor(p) for p in chunk]
                self.import_chunk(conn=conn,params=chunk)
            if self.autocommit:
                conn.commit()

    def import_chunk(self,conn,params):
        if isinstance(self.query,str):
            conn.execute(
                text(self.query),
                params,
            )
        elif isinstance(self.query,list):
            for q in self.query:
                conn.execute(
                    text(q),
                    params,
                )
        else:
            msg = f"Unsupported query type:{type(self.query)}"
            raise NotImplementedError(msg)



class Oeamdb:
    """Wrapper class."""


    sql_base = Base

    def __init__(
        self,
        engine_url="sqlite:///oeamdb.db",
        engine=None,
        **kwargs,
    ):
        self.engine_url = engine_url
        if engine is not None:
            self.engine=engine
        else:
            self.init_engine()
        self.sql_base.metadata.create_all(self.engine,checkfirst=True)

    def init_engine(self )-> None:
        """Initiating the SQLAlchemy engine if not existing."""
        if not hasattr(self, "engine"):
            if self.engine_url.startswith("sqlite:"):
                connect_args = {
                    "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                }
            else:
                connect_args = {}
            self.engine_cfg = {
                "url": self.engine_url, "connect_args": connect_args,
            }
            self.engine = create_engine(**self.engine_cfg)

    def drop_all(self)-> None:
        """Clean slate for the database."""
        self.sql_base.metadata.drop_all(self.engine)
