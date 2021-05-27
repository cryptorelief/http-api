from geoalchemy2 import Geometry  # <= not used but must be imported
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from credentials import USERNAME, PASSWORD, HOSTNAME, PORT, DB_NAME
from contextlib import contextmanager

engine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))

metadata = sqlalchemy.MetaData(engine)
metadata.reflect()

Base = automap_base(metadata=metadata)
Base.prepare(engine)

# All tables that can be queried using the API
Demand = Base.classes.Demand
Supply = Base.classes.Supply
Raw = Base.classes.Raw
Matches = Base.classes.Matches
Auth = Base.classes.Auth
UserLog = Base.classes.UserLog
Contact = Base.classes.Contact

"""
class Demand(Base):
    __table__ = sqlalchemy.Table("Demand", metadata)

class Supply(Base):
    __table__ = sqlalchemy.Table("Supply", metadata)

class Matches(Base):
    __table__ = sqlalchemy.Table("Matches", metadata)

class Raw(Base):
    __table__ = sqlalchemy.Table("Raw", metadata)

class Auth(Base):
    __table__ = sqlalchemy.Table("Auth", metadata)

class UserLog(Base):
    __table__ = sqlalchemy.Table("UserLog", metadata)
"""

@contextmanager
def get_session():
    Session = sqlalchemy.orm.sessionmaker(engine)
    session = Session()

    try:
        yield session
    except:
        session.rollback()
        raise
    else:
        session.commit()
