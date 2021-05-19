from flask import Flask, request
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
from flask_jwt_extended import create_access_token,get_jwt_identity,jwt_required,JWTManager
from werkzeug.security import safe_str_cmp
import pandas as pd
from datetime import datetime

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'a39c6c92e2bd7d37ef508a571cbc92f8' # TODO: To be changed into env_variable
jwt = JWTManager(app)

pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))
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

class Auth(Base):
    __table__ = sqlalchemy.Table("Auth", metadata)

# Session object to talk with the db
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

def filter(table):
    args = request.args.to_dict()
    # special query parameters that are not table columns
    verified_after = args.pop("verified_after", None)
    after = args.pop("after", None)
    before = args.pop("before", None)
    try:
        verified_after = pd.to_datetime(verified_after)
        after = pd.to_datetime(after)
        before = pd.to_datetime(before)
    except:
        return "Invalid datetime format"
    limit = args.pop("limit", None)
    try:
        # construct the query
        s = session.query(table).filter_by(**args)
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


def insert_or_update(table):
    """Checks whether there is an id. If there is an id, """
    data = request.get_json()
    identifier = data.pop('id', None)
    if identifier:
        # user is attempting to update a record
        return update(table, data, identifier)
    else:
        # user is attempting to insert a record
        return insert(table, data)


def insert(table, data):
    try:
        record = table(**data)
        session.add(record)
        session.commit()
        session.refresh(record)
        results = [record]
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def update(table, data, identifier):
    try:
        query = session.query(table).filter_by(id=identifier)
        record = query.first()
        if record is None:
            return "Could not find record"
        query.update(data)
        # fetch newly updated record
        session.commit()
        session.refresh(record)
    except (SQLAlchemyError, ValueError) as e:
        return str(e)
    else:
        results = [record]
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


@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.id


@jwt.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    return Auth.query.filter_by(id=identity).one_or_none()


@app.post("/login")
def login():
    queries = request.args.to_dict()
    username = queries.get("username")
    password = queries.get("password")
    if(username is not None and password is not None):
        user = session.query(Auth).filter_by(username=username).first()
        if(user):
            if(safe_str_cmp(user.password.encode('utf-8'),password.encode('utf-8'))):
                access_token = create_access_token(identity=user)
                return {"access_token": access_token},200
            else:
                return {"error": "Password does not match."},401
        else:
            return {"error": "Username not found."},401
    else:
        return {"error": "Please provide both Username and Password."},401


@app.get("/requests")
def get_demand():
    results = filter(Demand)
    return generate_response(results)


@app.get("/supply")
def get_supply():
    results = filter(Supply)
    return generate_response(results)


@app.get("/matches")
def get_matches():
    results = filter(Matches)
    return generate_response(results)


@app.get("/raw")
def get_raw():
    results = filter(Raw)
    return generate_response(results)


@jwt_required()
@app.post("/requests")
def post_demand():
    results = insert_or_update(Demand)
    return generate_response(results)


@jwt_required()
@app.post("/supply")
def post_supply():
    results = insert_or_update(Supply)
    return generate_response(results)


@jwt_required()
@app.post("/matches")
def post_matches():
    results = insert_or_update(Matches)
    return generate_response(results)


@jwt_required()
@app.post("/raw")
def post_raw():
    results = insert_or_update(Raw)
    return generate_response(results)


if __name__=="__main__":
    app.run(debug=True)
