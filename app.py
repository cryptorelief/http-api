from flask import Flask, request
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import SQLAlchemyError
from flask_jwt_extended import create_access_token,get_jwt_identity,jwt_required,JWTManager
from werkzeug.security import safe_str_cmp
import pandas as pd
from datetime import datetime
from db import Demand, Supply, Raw, Matches, UserLog, Auth, get_session

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'a39c6c92e2bd7d37ef508a571cbc92f8' # TODO: To be changed into env_variable
app.config["JWT_TOKEN_LOCATION"] = ["headers", "query_string"]
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = False

jwt = JWTManager(app)


def obj_to_dict(obj):
    return dict([(k,v) for (k,v) in vars(obj).items() if not k.startswith("_")])


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
        results = []
        with get_session() as session:
            s = session.query(table).filter_by(**args)
            if after:
                s = s.filter(table.last_updated>after)
            if before:
                s = s.filter(table.last_updated<before)
            if verified_after:
                s = s.filter(table.last_verified_on>=verified_after)
            # apply sensible defaults for ordering
            s = s.order_by(text('verified desc, last_verified_on desc, last_updated desc'))
            # NOTE: limit has to be applied after ordering
            if limit:
                s = s.limit(limit)
            # run query
            results = s.all()
            results = [obj_to_dict(result) for result in results]            
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def insert_or_update(table):
    """Checks whether there is an id. If there is an id, """
    data = request.get_json()
    data.pop('jwt', None)
    identifier = data.pop('id', None)
    if identifier:
        # user is attempting to update a record
        return update(table, data, identifier)
    else:
        # user is attempting to insert a record
        return insert(table, data)


def insert(table, data):
    try:
        results = []
        with get_session() as session:
            user = session.query(Auth).filter_by(id=get_jwt_identity()).first()
            record = table(**data)
            ## create User Log
            # {foreign key column: id of updated record}
            fk = {"{}_id".format(table.__name__.lower()): record.id}
            # update info (username, timestamp)
            log = {"username": user.username, "last_updated": datetime.now()}
            # all update info
            log.update(fk)
            # find if last updated already exists for username, <table>_id
            existing = session.query(UserLog).filter_by(username=user.username, **fk).first()
            if existing:
                session.query(UserLog).update(log)
            else:
                session.add(UserLog(**log))
            session.commit()
            ## Insert new record into table
            session.add(record)
            session.commit()
            session.refresh(record)
            results = [obj_to_dict(record)]
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def update(table, data, identifier):
    try:
        results = []
        with get_session() as session:
            user = session.query(Auth).filter_by(id=get_jwt_identity()).first()
            query = session.query(table).filter_by(id=identifier)
            record = query.first()
            data.pop('jwt', None)
            if record is None:
                return "Could not find record"
            ## create User Log
            # {foreign key column: id of updated record}
            fk = {"{}_id".format(table.__name__.lower()): record.id}
            # update info (username, timestamp)
            log = {"username": user.username, "last_updated": datetime.now()}
            # all update info
            log.update(fk)
            # find if last updated already exists for username, <table>_id
            existing = session.query(UserLog).filter_by(username=user.username, **fk).first()
            if existing:
                session.query(UserLog).update(log)
            else:
                session.add(UserLog(**log))
            session.commit()
            session.flush()
            ## Update table with new data
            query.update(data)
            session.commit()
            # fetch newly updated record
            session.refresh(record)
            results = [obj_to_dict(record)]
        return results
    except (SQLAlchemyError,  ValueError) as e:
        return str(e)


def generate_response(results):
    """Converts results (empty list, list of vars (incl. internal vars), or string) into a response-friendly object"""
    # if result was a string, it's an error
    if isinstance(results,str):
        return {"error": results.replace("\"", "'")}, 400
    elif not isinstance(results, list):
        return {"error": "Invalid data received from server", "data": results}, 503
    # get all vars/children of the SQLAlchemy object
    # this also contains internal Python variables, so we remove those
    return {"data": results}


@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.id


@jwt.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    with get_session() as session:
        return session.query(Auth).filter_by(id=identity).one_or_none()


@app.post("/login")
def login():
    queries = request.args.to_dict()
    username = queries.get("username")
    password = queries.get("password")
    if(username is not None and password is not None):
        with get_session() as session:
            user = session.query(Auth).filter_by(username=username).first()
        if(user):
            if(safe_str_cmp(user.password.encode('utf-8'),password.encode('utf-8'))):
                access_token = create_access_token(identity=user)
                return {"access_token": access_token}, 200
            else:
                return {"error": "Authentication failed"}, 401
        else:
            return {"error": "Authentication failed"}, 401
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


@app.post("/requests")
@jwt_required()
def post_demand():
    results = insert_or_update(Demand)
    return generate_response(results)


@app.post("/supply")
@jwt_required()
def post_supply():
    results = insert_or_update(Supply)
    return generate_response(results)


@app.post("/matches")
@jwt_required()
def post_matches():
    results = insert_or_update(Matches)
    return generate_response(results)


@app.post("/raw")
@jwt_required()
def post_raw():
    results = insert_or_update(Raw)
    return generate_response(results)


if __name__=="__main__":
    app.run(debug=True)
