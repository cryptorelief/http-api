import json
from flask import Flask,request
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError as err
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME

app = Flask(__name__)

pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))
Base = declarative_base()
metadata = sqlalchemy.MetaData(pengine)
metadata.reflect()

# All the tables in the database (probably would be renamed in the future)

class Demand(Base):
    __table__ = sqlalchemy.Table("Demand", metadata)

class Supply(Base):
    __table__ = sqlalchemy.Table("Supply", metadata)

class Matches(Base):
    __table__ = sqlalchemy.Table("Matches", metadata)

class Raw(Base):
    __table__ = sqlalchemy.Table("Raw", metadata)

# session object to talk with the db
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

# base_url = "api.covidbot.in"

def get_results(table):
    r_dict = request.args.to_dict()
    after = r_dict.pop("after", None)
    before = r_dict.pop("before", None)
    s = session.query(table).filter_by(**r_dict)
    if after is not None:
        s = s.filter(table.datetime>after)
    if before is not None:
        s = s.filter(table.datetime<before)
    results = s.all()
    return results

def results_to_json(results):
    if isinstance(results,list) is False:
        return json.dumps(results)
    all_results = [vars(result) for result in results]
    final_result = []
    for result in all_results:
        final_result.append(dict([(k,v) for (k,v) in result.items() if not k.startswith("_")]))
    final_result_json = json.dumps(final_result,indent=4,default=str)
    return final_result_json

def put_results(table):
    r_dict = request.args.to_dict()
    try:
        d = table(**r_dict)
        session.add(d)
        session.commit()
        results = [d]
        return results,200
    except Exception as e:
        return (str(e)),400

def update_results(table):
    r_dict = request.args.to_dict()
    try:
        s = session.query(Supply).filter_by(external_id=r_dict["external_id"]).first()
        if s is None:
            return [],400
        for k in r_dict:
            s.k = r_dict[k]
        session.commit()
        results = [s]
        return results,200
    except Exception as e:
        return (str(e)),400

@app.get("/request")
def get_demand():
    results = get_results(Demand)
    return results_to_json(results)

@app.get("/supply")
def get_supply():
    results = get_results(Supply)
    return results_to_json(results)

@app.get("/matches")
def get_matches():
    results = get_results(Matches)
    return results_to_json(results)

@app.get("/raw")
def get_raw():
    results = get_results(Raw)
    return results_to_json(results)

@app.put("/request")
def put_demand():
    results,status_code = put_results(Demand)
    return results_to_json(results),status_code

@app.put("/supply")
def put_supply():
    results,status_code = put_results(Supply)
    return results_to_json(results),status_code

@app.put("/raw")
def put_raw():
    results,status_code = put_results(Raw)
    return results_to_json(results),status_code

@app.put("/update/demand")
def update_demand():
    results,status_code = update_results(Demand)
    return results_to_json(results)

@app.put("/update/supply")
def update_supply():
    results,status_code = update_results(Supply)
    return results_to_json(results)

@app.put("/update/matches")
def update_matches():
    results,status_code = update_results(Matches)
    return results_to_json(results)

@app.put("/update/raw")
def update_raw():
    results,status_code = update_results(Raw)
    return results_to_json(results)

if __name__=="__main__":
    app.run(debug=True)
