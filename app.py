from flask import Flask, request
from sqlalchemy import func, and_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import SQLAlchemyError
from flask_jwt_extended import create_access_token,get_jwt_identity,jwt_required,JWTManager
from werkzeug.security import safe_str_cmp
import pandas as pd
from datetime import datetime
from db import Demand, Supply, Raw, Matches, UserLog, Auth, Contact, Locations, Volunteer
from db import get_session
from sqlalchemy.sql.expression import desc, nulls_last
from credentials import JWT_KEY

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = JWT_KEY
app.config["JWT_TOKEN_LOCATION"] = ["headers", "query_string"]
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = False
app.config['APPLICATION_ROOT'] = "/core"
jwt = JWTManager(app)


def obj_to_dict(obj):
    return dict([(k,v) for (k,v) in vars(obj).items() if not k.startswith("_")])


def search(table):
    args = request.args.to_dict()
    # special query parameters that are not table columns:
    # verified_after, (updated) after, (updated) before, limit
    verified_after = args.pop("verified_after", None)
    after = args.pop("after", None)
    before = args.pop("before", None)
    # validate special fields
    try:
        # `pd.to_datetime(None)` returns `None` so this is okay
        verified_after = pd.to_datetime(verified_after)
        after = pd.to_datetime(after)
        before = pd.to_datetime(before)
    except:
        return "Invalid datetime format"
    limit = args.pop("limit", "")
    if limit and not limit.isdigit():
        return "limit must be an integer"

    try:
        # construct the query
        results = []
        with get_session() as session:
            s = session.query(table).filter_by(**args)
            # optional/special query parameters
            if after:
                s = s.filter(table.last_updated>=after)
            if before:
                s = s.filter(table.last_updated<before)
            if verified_after:
                s = s.filter(table.last_verified_on>=verified_after)
            # sensible defaults for ordering: verified, last_verified_on, last_updated
            if table == Supply:
                s = s.order_by(nulls_last(desc(Supply.verified)), nulls_last(desc(Supply.last_verified_on)), nulls_last(desc(Supply.last_updated)))
            # NOTE: limit has to be applied after ordering
            if limit:
                s = s.limit(limit)
            # run query
            results = s.all()
            results = [obj_to_dict(result) for result in results]
        return results
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def location_search():
    args = request.args.to_dict()
    limit = args.pop("limit", "")
    if limit and not limit.isdigit():
        return "limit must be an integer"
    name = args.pop("name", "")

    try:
        # construct the query
        results = []
        with get_session() as session:
            s = session.query(Locations).filter_by(**args)
            # optional/special query parameters
            if name:
                s = s.filter(Locations.name.ilike(name))
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
    source = data.pop("source", "").lower()
    contact_lookup = {}
    if source == "telegram":
        tg_user_id = data.pop("tg_user_id", None)
        tg_user_handle = data.pop("tg_user_handle", None)
        if tg_user_id:
            contact_lookup['tg_user_id'] = tg_user_id
        if tg_user_handle:
            contact_lookup['user_handle'] = tg_user_handle
    # add source only if there's lookup data - otherwise skip
    try:
        results = []
        with get_session() as session:
            user = session.query(Auth).filter_by(id=get_jwt_identity()).first()
            if not user:
                return "Authentication failed"
            if contact_lookup:
                contact = session.query(Contact).filter_by(**contact_lookup).first()
                if not contact:
                    return f"Contact not found: {contact_lookup}"
                data.update(contact=contact)
            data['group_handle'] = "-1001367739196"
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
    except (SQLAlchemyError, ValueError) as e:
        return str(e)


def find_matches():
    args = request.args.to_dict()
    source = args.get("source", "").lower()
    contact = None
    # if source is telegram, we need to find/create the user
    if source == "telegram":
        tg_user_id = args.get("tg_user_id", "")
        tg_user_handle = args.get("tg_user_handle", "")
        with get_session() as session:
            # try to find telegram contact using tg_user_id
            if tg_user_id:
                contact = session.query(Contact).filter_by(tg_user_id=tg_user_id, source="telegram").first()
            # try to find telegram contact using handle
            if (not contact) and tg_user_handle:
                contact = session.query(Contact).filter_by(user_handle=tg_user_handle, source="telegram").first()
            # if still no contact found, create new contact
            if not contact:
                if not (tg_user_id or tg_user_handle):
                    return "Error: Not enough contact information"
                contact = Contact(source="telegram", user_handle=tg_user_handle, tg_user_id=tg_user_id, bot_activated=True)
                session.add(contact)
                session.commit()
                session.refresh(contact)
                return []
            if not contact:
                return "Contact not found"
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
                supplies = session.query(Supply).filter(Supply.id.in_(supply_ids), Supply.phone.isnot(None)).all()
                if not supplies:
                    return "No new results found"
                key = ", ".join(filter(None, [demand.resource, demand.category, demand.location_raw, demand.phone]))
                if not key:
                    return "Invalid request detected"
                value = [": ".join(filter(None, [supply.title, supply.phone])) for supply in supplies]
                new_matches.append(dict([(key, value)]))
                for match in matches:
                    match.sent = True
                session.bulk_save_objects(matches)
            if new_matches:
                return new_matches
            else:
                return "No new results found"

def generate_response(results, status_code=400):
    """Converts results (empty list, list of vars (incl. internal vars), or string) into a response-friendly object"""
    # if result was a string, it's an error
    if isinstance(results,str):
        return {"error": results.replace("\"", "'")}, status_code
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
    args = request.args.to_dict()
    username = args.get("username")
    password = args.get("password")
    if not (username and password):
        return {"error": "Please provide both Username and Password"}, 401
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


@app.get("/requests")
def get_demand():
    results = search(Demand)
    return generate_response(results)


@app.get("/supply")
def get_supply():
    results = search(Supply)
    return generate_response(results)


@app.get("/location")
def get_locations():
    results = location_search()
    return generate_response(results)


@app.get("/matches")
def get_matches():
    results = find_matches()
    return generate_response(results)


@app.get("/rawdata")
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


@app.post("/volunteer")
@jwt_required()
def post_volunteer():
    results = insert_or_update(Volunteer)
    return generate_response(results)


@app.post("/rawdata")
@jwt_required()
def post_raw():
    results = insert_or_update(Raw)
    return generate_response(results)


if __name__=="__main__":
    app.run(debug=True)
