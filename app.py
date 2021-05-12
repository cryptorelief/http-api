#Author: Pranav sqlalchemystry
#DateTime: 2021-05-12 20:15:49.881593 IST

import sys
import os
sys.path.append("/opt/anaconda3/lib/python3.7/site-packages/")

from flask import Flask,request
from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.dialects.postgresql import UUID

from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME)

# create an engine
pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# reflect current database engine to metadata
metadata = sqlalchemy.MetaData(pengine)
metadata.reflect()

class Demand(Base):
    __table__ = sqlalchemy.Table("Demands_old", metadata)

# class Supply(Base):
#     __table__ = sqlalchemy.Table("Supply", metadata)
#
# class Demand(Base):
#     __table__ = sqlalchemy.Table("Demands_old", metadata)
#
# class Demand(Base):
#     __table__ = sqlalchemy.Table("Demands_old", metadata)


# call the session maker factory
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

# filter a record
# session.query(User).filter(User.id==1).first()


db = SQLAlchemy(app)

# class Demand(db.Model):
#     __tablename__='Demands_old'
#     id = db.Column(UUID(as_uuid=True), primary_key=True)
#     help_needed = db.Column(db.String(200))


# base_url = "api.covidbot.in"
@app.get("/request")
def get_demand():
    r = request.args
    r_dict = r.to_dict()
    after = r_dict.pop("after", None)
    before = r_dict.pop("before", None)

    s = session.query(Demand).filter_by(r_dict)
    if after is not None:
        s = s.filter(Demand.datetime>after)
    if before is not None:
        s = s.filter(Demand.datetime<before)
    results = s.all()
    all_results = [vars(result) for result in results]
    final_result = []
    for result in all_results:
        final_result.append(dict([(k,v) for (k,v) in result.items() if not k.startswith("_")]))
    final_result_json = json.dumps(final_result,indent=4,default=str)

@app.get("/supply")
def get_supply():
    pass

@app.get("/matches")
def get_matches():
    pass

@app.get("/raw")
def get_raw():
    pass

@app.put("/request")
def put_demand():
    pass

@app.put("/supply")
def put_supply():
    pass

@app.put("/raw")
def put_raw():
    pass


# @app.post("/")
# def func():
#     pass


if __name__=="__main__":
    app.run(debug=True)
