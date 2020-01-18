import json
import dateutil
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ReplayRecord(Base):
    __tablename__ = 'replay'
    hash = Column(String, primary_key=True)
    mmrs = Column(String)
    ranks = Column(String)
    match_date = Column(DateTime)
    upload_date = Column(DateTime)
    playlist = Column(Integer)

    @staticmethod
    def create(from_: dict):
        record = ReplayRecord()
        record.hash = from_['hash']
        record.mmrs = json.dumps(from_['mmrs'])
        record.ranks = json.dumps(from_['ranks'])
        record.match_date = dateutil.parser.parse(from_['match_date'])
        record.upload_date = dateutil.parser.parse(from_['upload_date'])
        record.playlist = 28
        return record


class Database(object):

    def __init__(self, dir: str):
        self.engine = create_engine(f'sqlite:///{dir}/replays.sqlite')
        Base.metadata.create_all(self.engine)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def get(self, hash: str):
        return self.Session().query(ReplayRecord).get(hash)

    def add(self, record: ReplayRecord):
        self.Session().add(record)
        self.Session().commit()
