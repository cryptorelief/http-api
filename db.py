from geoalchemy2 import Geometry  # <= not used but must be imported
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from credentials import USERNAME, PASSWORD, HOSTNAME, PORT, DB_NAME
from contextlib import contextmanager

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DB_NAME}")

metadata = sqlalchemy.MetaData(engine)
metadata.reflect()

Base = automap_base(metadata=metadata)
Base.prepare(engine)

# All db tables can be queried using the API
Demand = Base.classes.Demand
Supply = Base.classes.Supply
Raw = Base.classes.Raw
Matches = Base.classes.Matches
Auth = Base.classes.Auth
UserLog = Base.classes.UserLog
Contact = Base.classes.Contact
Locations = Base.classes.Locations
Volunteer = Base.classes.Volunteer


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
