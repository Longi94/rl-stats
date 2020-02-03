import json
import dateutil
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Float, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ItemsExtracted(Base):
    __tablename__ = 'items_extracted'
    id = Column(Integer, primary_key=True, index=True)
    hash = Column(String, unique=True, index=True)
    avg_mmr = Column(Float)
    avg_rank = Column(Integer, index=True)
    map = Column(Integer)
    match_date = Column(DateTime, index=True)
    children = relationship('ItemRecord', back_populates='parent')
    goals = relationship('GoalRecord', back_populates='parent')
    orange_winner = Column(Boolean)

    @staticmethod
    def create(from_: dict):
        record = ItemsExtracted()
        record.hash = from_['hash']
        record.map = from_['map']
        record.avg_mmr = from_['avg_mmr']
        record.avg_rank = from_['avg_rank']
        return record


class ItemRecord(Base):
    __tablename__ = 'item'
    parent_id = Column(Integer, ForeignKey('items_extracted.id'), primary_key=True, index=True)
    parent = relationship('ItemsExtracted', back_populates='children')
    player_id = Column(String, primary_key=True, index=True)
    frame_get = Column(Integer, primary_key=True, index=True)
    frame_use = Column(Integer)
    item = Column(Integer, index=True)
    use_x = Column(Float)
    use_y = Column(Float)
    use_z = Column(Float)
    wait_time = Column(Float)
    is_kickoff = Column(Boolean)
    is_orange = Column(Boolean)

    @staticmethod
    def create(from_: dict):
        record = ItemRecord()
        record.player_id = from_['player_id']
        record.frame_get = from_['frame_get']
        record.frame_use = from_['frame_use']
        record.item = from_['item']
        record.use_x = from_['use_x']
        record.use_y = from_['use_y']
        record.use_z = from_['use_z']
        record.wait_time = from_['wait_time']
        record.is_kickoff = from_['is_kickoff']
        record.is_orange = from_['is_orange']
        return record


class GoalRecord(Base):
    __tablename__ = 'goal'
    parent_id = Column(Integer, ForeignKey('items_extracted.id'), primary_key=True, index=True)
    parent = relationship('ItemsExtracted', back_populates='goals')
    player_id = Column(String, primary_key=True)
    frame = Column(Integer, primary_key=True)
    item = Column(Integer, index=True)
    pre_item = Column(Boolean)
    is_orange = Column(Boolean)

    @staticmethod
    def create(from_: dict):
        record = GoalRecord()
        record.player_id = from_['player_id']
        record.item = from_['item']
        record.is_orange = from_['is_orange']
        return record


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

    def as_dict(self):
        return {
            'hash': self.hash,
            'mmrs': self.mmrs,
            'ranks': self.ranks,
            'match_date': self.match_date,
            'upload_date': self.upload_date,
            'playlist': self.playlist
        }


class Database(object):

    def __init__(self, dir: str):
        self.engine = create_engine(f'sqlite:///{dir}/replays.sqlite', connect_args={'timeout': 60})
        Base.metadata.create_all(self.engine)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def get_existing_hashes(self):
        return set(map(lambda x: x.hash, self.Session().query(ReplayRecord)))

    def add(self, record: ReplayRecord):
        self.Session().add(record)

    def close(self):
        self.Session().close()
        self.engine.dispose()

    def get_replays(self):
        return self.Session().query(ReplayRecord)

    def add_event(self, record: ItemRecord):
        self.Session().add(record)

    def commit(self):
        self.Session().commit()
