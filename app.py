from flask import Flask, request
from sqlalchemy import func, and_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import SQLAlchemyError
from flask_jwt_extended import create_access_token,get_jwt_identity,jwt_required,JWTManager
from werkzeug.security import safe_str_cmp
import pandas as pd
from datetime import datetime
from db import Demand, Supply, Raw, Matches, UserLog, Auth, Contact, get_session
from sqlalchemy.sql.expression import desc, nulls_last
from credentials import JWT_KEY

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = JWT_KEY
app.config["JWT_TOKEN_LOCATION"] = ["headers", "query_string"]
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = False
jwt = JWTManager(app)


def obj_to_dict(obj):
    return dict([(k,v) for (k,v) in vars(obj).items() if not k.startswith("_")])


def search(table):
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
            if table == Supply:
                s = s.order_by(and_(nulls_last(desc(Supply.verified)), nulls_last(desc(Supply.last_verified_on)), nulls_last(desc(Supply.last_updated))))
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


def find_matches():
    args = request.args.to_dict()
    source = args.get("source", "")
    if source.lower()=="telegram":
        tg_user_id = args.get("tg_user_id", "")
        handle = args.get("user_handle", "")
        contact = None
        with get_session() as session:
            if tg_user_id:
                contact = session.query(Contact).filter_by(tg_user_id=tg_user_id).first()
            if not (tg_user_id or contact) and handle:
                contact = session.query(Contact).filter_by(user_handle=handle).first()
            if not contact:
                contact = Contact(source="telegram", user_handle=handle, tg_user_id=tg_user_id, bot_activated=True)
                with get_session() as session:
                    session.add(contact)
                    session.commit()
                    session.refresh(contact)
                    return []
            demands = session.query(Demand).filter_by(contact=contact).order_by(nulls_last(desc(Demand.datetime))).limit(100).all()
            if not demands:
                return "You have not submitted any requests. Please click on /find to submit a request"
            new_matches = []
            for demand in demands:
                session.query(func.match_demand_to_new_supply(demand.id)).all()
                matches = session.query(Matches).filter_by(demand=demand, sent=False).order_by(desc(Matches.created_on)).limit(10).all()
                if not matches:
                    continue
                supply_ids = [match.supply_id for match in matches]
                supplies = session.query(Supply).filter(Supply.id.in_(supply_ids)).all()
                if not supplies:
                    return "No new results found"
                key = ", ".join(filter(None, [demand.resource, demand.category, demand.location_text, demand.phone]))
                if not key:
                    return "Invalid request detected"
                value = ["{}: {}".format(supply.title, supply.phone) for supply in supplies]
                new_matches.append(dict([(key, value)]))
                for match in matches:
                    match.sent = True
                session.bulk_save_objects(matches)
            if new_matches:
                return new_matches
            else:
                return "No new results found"

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
    results = search(Demand)
    return generate_response(results)


@app.get("/supply")
def get_supply():
    results = search(Supply)
    return generate_response(results)


@app.get("/results")
def get_matches():
    results = find_matches()
    return generate_response(results)


@app.get("/raw")
def get_raw():
    results = search(Raw)
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


@app.post("/raw")
@jwt_required()
def post_raw():
    results = insert_or_update(Raw)
    return generate_response(results)


if __name__=="__main__":
    app.run(debug=True)
