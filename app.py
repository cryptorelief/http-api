from flask import Flask, request
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
import pandas as pd
from datetime import datetime
app = Flask(__name__)

pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME), pool_use_lifo=True, pool_pre_ping=True)
Base = declarative_base()
metadata = sqlalchemy.MetaData(pengine)
metadata.reflect()

# All tables that can be queried using the API
class Demand(Base):
    __table__ = sqlalchemy.Table("Demand", metadata)

class Supply(Base):
    __table__ = sqlalchemy.Table("Supply", metadata)

class Matches(Base):
    __table__ = sqlalchemy.Table("Matches", metadata)

class Raw(Base):
    __table__ = sqlalchemy.Table("Raw", metadata)

# Session object to talk with the db
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

def filter_data(table):
    r_dict = request.args.to_dict()
    # special query parameters that are not table columns
    verified_after = r_dict.pop("verified_after", None)
    after = r_dict.pop("after", None)
    before = r_dict.pop("before", None)
    try:
        verified_after = pd.to_datetime(verified_after)
        after = pd.to_datetime(after)
        before = pd.to_datetime(before)
    except:
        return "Invalid datetime format"
    limit = r_dict.pop("limit", None)
    try:
        # construct the query
        s = session.query(table).filter_by(**r_dict)
        if after:
            s = s.filter(table.last_updated>after)
        if before:
            s = s.filter(table.last_updated<before)
        if verified_after:
            s = s.filter(table.last_verified_on>=verified_after)
        # apply sensible defaults for ordering
        s = s.order_by(sqlalchemy.text('verified desc, last_verified_on desc, last_updated desc'))
        # NOTE: limit has to be applied after ordering
        if limit:
            s = s.limit(limit)
        # run query
        results = s.all()
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def insert_data(table):
    r_dict = request.args.to_dict()
    try:
        d = table(**r_dict)
        session.add(d)
        session.commit()
        results = [d]
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def update_data(table):
    r_dict = request.args.to_dict()
    try:
        s = session.query(Supply).filter_by(external_uuid=r_dict["external_uuid"]).first()
        if s is None:
            return []
        for k in r_dict:
            s.k = r_dict[k]
        session.commit()
    except (SQLAlchemyError, ValueError) as e:
        return str(e)
    else:
        results = [s]
        return results


def generate_response(results):
    """Converts results (empty list, list of vars (incl. internal vars), or string) into a response-friendly object"""
    # if result was a string, it's an error
    if isinstance(results,str):
        return {"error": results.replace("\"", "'")}, 400
    elif not isinstance(results, list):
        return {"error": "Invalid data received from server", "data": results}, 503
    # get all vars/children of the SQLAlchemy object
    all_results = [vars(result) for result in results]
    # this also contains internal Python variables, so we remove those
    final_result = []
    for result in all_results:
        final_result.append(dict([(k,v) for (k,v) in result.items() if not k.startswith("_")]))
    return {"data": final_result}


@app.get("/requests")
def get_demand():
    results = filter_data(Demand)
    return generate_response(results)


@app.get("/supply")
def get_supply():
    results = filter_data(Supply)
    return generate_response(results)


@app.get("/matches")
def get_matches():
    results = filter_data(Matches)
    return generate_response(results)


@app.get("/raw")
def get_raw():
    results = filter_data(Raw)
    return generate_response(results)


@app.put("/requests")
def put_demand():
    results,status_code = insert_data(Demand)
    return generate_response(results)


@app.put("/supply")
def put_supply():
    results,status_code = insert_data(Supply)
    return generate_response(results)


@app.put("/raw")
def put_raw():
    results,status_code = insert_data(Raw)
    return generate_response(results)


@app.put("/update/demand")
def update_demand():
    results,status_code = update_data(Demand)
    return generate_response(results)


@app.put("/update/supply")
def update_supply():
    results,status_code = update_data(Supply)
    return generate_response(results)


@app.put("/update/matches")
def update_matches():
    results,status_code = update_data(Matches)
    return generate_response(results)


@app.put("/update/raw")
def update_raw():
    results,status_code = update_data(Raw)
    return generate_response(results)


if __name__=="__main__":
    app.run(debug=True)
