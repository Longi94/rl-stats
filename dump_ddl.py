from sqlalchemy import create_engine
from database import Base


def metadata_dump(sql, *multiparams, **params):
    # print or write to log or file etc
    print(sql.compile(dialect=engine.dialect))


engine = create_engine('sqlite://', strategy='mock', executor=metadata_dump)
Base.metadata.create_all(engine)
