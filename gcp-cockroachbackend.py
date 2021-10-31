import os
# import pymongo
import json
import random
import psycopg2




def connector():
    # cockroachstring = "dbname='wet-dingo-838.defaultdb' user='muntaser' password='rootpassword' host='free-tier.gcp-us-central1.cockroachlabs.cloud' port='26257'"
    cockroachstring = os.environ.get('COCKROACHSTR')
    conn=psycopg2.connect(cockroachstring)
    return conn



def initialize(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, username STRING, email STRING, userpassword STRING, useraddress STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS events (id INT PRIMARY KEY, userid STRING, name STRING, location STRING, starttime STRING, ts STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS alarms (id INT PRIMARY KEY, userid STRING, name STRING, location STRING, etime STRING, atime STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS preptime (id INT PRIMARY KEY, userid STRING, time STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS vitals (id INT PRIMARY KEY, userid STRING, pulse STRING, spo2 STRING, ts STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS thermostat (id INT PRIMARY KEY, userid STRING, temp STRING)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS system (id INT PRIMARY KEY, donated STRING, used STRING, balance STRING, impacts STRING)"
        )
        # cur.execute("UPSERT INTO users (id, email, userpassword, usertype, name) VALUES (1, 'jon@fisherman.com', 'password1', 'fisherman', 'jon stewart'), (2, 'joe@gmail.com', 'password1', 'customer', 'joe someone')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()



def add_events(conn, userid, name, location, starttime, ts):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM events")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        helperid = "-1"
        status = "created"
        cur.execute("UPSERT INTO events (id, userid, name, location, starttime, ts) VALUES (" + i +", '" + userid +"', '" + name + "', '" + location + "', '" + starttime +"', '" + ts +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")



def add_alarms(conn, userid, name, location, etime, atime):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM alarms")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        helperid = "-1"
        status = "created"
        cur.execute("UPSERT INTO alarms (id, userid, name, location, etime, atime) VALUES (" + i +", '" + userid +"', '" + name + "', '" + location + "', '" + etime +"', '" + atime +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")


def add_preptime(conn, userid, time):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM preptime")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        helperid = "-1"
        status = "created"
        cur.execute("UPSERT INTO preptime (id, userid, time) VALUES (" + i +", '" + userid +"', '" + time +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")


def add_vitals(conn, userid, pulse, spo2, ts):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM vitals")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        helperid = "-1"
        status = "created"
        cur.execute("UPSERT INTO vitals (id, userid, pulse, spo2, ts) VALUES (" + i +", '" + userid +"', '" + pulse + "', '" + spo2 + "', '" + ts +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")




def add_thermostat(conn, userid, temp):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM thermostat")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        helperid = "-1"
        status = "created"
        cur.execute("UPSERT INTO thermostat (id, userid, temp) VALUES (" + i +", '" + userid +"', '" + temp +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")




def settemp(conn, id, temp):
    # newstatus = "pending"
    with conn.cursor() as cur:
        cur.execute( "UPDATE thermostat SET temp = %s WHERE id = %s", (temp, id));
        #  "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, frm)
        conn.commit()

    return 0



def setprep(conn, id, time):
    # newstatus = "pending"
    with conn.cursor() as cur:
        cur.execute( "UPDATE preptime SET time = %s WHERE id = %s", (time, id));
        #  "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, frm)
        conn.commit()

    return 0





def acceptedtask(conn, taskid):
    newstatus = "accepted"
    with conn.cursor() as cur:
        cur.execute( "UPDATE tasks SET status = %s WHERE id = %s", (newstatus, taskid));
        #  "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, frm)
        conn.commit()

    return 0


def completedtask(conn, taskid):
    newstatus = "completed"
    with conn.cursor() as cur:
        cur.execute( "UPDATE tasks SET status = %s WHERE id = %s", (newstatus, taskid));
        #  "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, frm)
        conn.commit()

    return 0




def add_donation(conn, corpid, amount, date):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM donations")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)

        st = "donated"
        
        cur.execute("UPSERT INTO donations (id, corpid, amount, date, status) VALUES (" + i +", '" + corpid + "', '" + amount + "', '" + date +"', '"  + st +"')")
        conn.commit()

        cur.execute("SELECT id, donations FROM corporations")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        newdon = amount

        for row in rows:
            if row[0] == corpid:
                oldon = int(row[1])
                nd = oldon + int(amount)
                newdon =  str(nd)

        cur.execute( "UPDATE corporations SET donations = %s WHERE id = %s", (newdon, corpid));
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)

        cur.execute("SELECT id, donated, balance FROM system")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        newdon = amount
        newbal = amount

        for row in rows:
            if row[0] == "0":
                oldon = int(row[1])
                oldbal = int(row[2])
                nd = oldon + int(amount)
                newdon =  str(nd)
                ob = oldbal + int(amount)
                newbal = str(ob)

        cur.execute( "UPDATE system SET donated = %s WHERE id = %s", (newdon, "0"));
        conn.commit()
        cur.execute( "UPDATE system SET balance = %s WHERE id = %s", (newbal, "0"));
        conn.commit()
            



    conn.commit()
    return i
    # print ("user added")



def add_users(conn, uname, pw, uemail, uaddress):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        i = 1
        for row in rows:
            i = i + 1
        i = str(i)
        
        cur.execute("UPSERT INTO users (id, email, userpassword, username, useraddress) VALUES (" + i +", '" + uemail + "', '" + pw +"', '" + uname +"', '" + uaddress +"')")
        # logging.debug("create_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    return i
    # print ("user added")


def login(conn, uemail, pw):
    with conn.cursor() as cur:
        cur.execute("SELECT id, email, userpassword, username, useraddress FROM users")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        for row in rows:
            # print(row)
            # print (type(row))
            if row[1] == uemail and row[2] == pw:
                # print ("found")
                return True, row[0], row[3], row[4], row[5]
        return False, 'none', 'none', '-1', '-1' 


def getuserbyid(conn, uid):
    with conn.cursor() as cur:
        cur.execute("SELECT id, email, userpassword, username, useraddress FROM users")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        for row in rows:
            # print(row)
            # print (type(row))
            if row[0] == int(uid):
                # print ("found")
                return True, row[0], row[1], row[3], row[4]
        return False, 'none', 'none', '-1', '-1'


def getevents(conn, userid):
    with conn.cursor() as cur:
        cur.execute("SELECT id, userid, name, location, starttime, ts FROM events")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        tasks = []

        for row in rows:
            if row[1] != userid:
                continue
             
            place = {}
            place['id'] = row[0]
            place['userid'] = row[1]
            place['name'] = row[2]
            place['location'] = row[3]
            place['starttime'] = row[4]
            place['ts'] = row[5]

            tasks.append(place)

        return tasks 



def getalarms(conn, userid):
    with conn.cursor() as cur:
        cur.execute("SELECT id, userid, name, location, etime, atime FROM alarms")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        tasks = []

        for row in rows:
            if row[1] != userid:
                continue
             
            place = {}
            place['id'] = row[0]
            place['userid'] = row[1]
            place['name'] = row[2]
            place['location'] = row[3]
            place['etime'] = row[4]
            place['atime'] = row[5]

            tasks.append(place)

        return tasks 






def gettemp(conn, userid):
    with conn.cursor() as cur:
        cur.execute("SELECT id, userid, temp FROM thermostat")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        tasks = []

        for row in rows:
            if row[1] != userid:
                continue
             
            place = {}
            place['id'] = row[0]
            place['userid'] = row[1]
            place['temp'] = row[2]
            tasks.append(place)

        return tasks 






def getplaces(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, placeaddress, lat, lon, type, img FROM places")
        # logging.debug("print_balances(): status message: %s", cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        # print(f"Balances at {time.asctime()}:")
        places = []

        for row in rows:
            place = {}
            place['id'] = row[0]
            place['name'] = row[1]
            place['address'] = row[2]
            place['lat'] = row[3]
            place['lon'] = row[4]
            place['type'] = row[5]
            place['image'] = row[6]

            places.append(place)

        return places 


def delete_users(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM defaultdb.users")
        # logging.debug("delete_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE users")
        # logging.debug("delete_accounts(): status message: %s", cur.statusmessage)
    conn.commit()

    print ("users table deleted")


def purgedb(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM defaultdb.users")
        # logging.debug("delete_accounts(): status message: %s", cur.statusmessage)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE users")
        # logging.debug("delete_accounts(): status message: %s", cur.statusmessage)
    conn.commit()

    print ("users table deleted")



def dummy(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    if request.method == 'OPTIONS':
        # Allows GET requests from origin https://mydomain.com with
        # Authorization header
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Credentials': 'true'
        }
        return ('', 204, headers)

    # Set CORS headers for main requests
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Credentials': 'true'
    }

    request_json = request.get_json()
    conn = connector()
    initialize(conn)

    retjson = {}

    action = request_json['action']

    if action == "createuser" :
        uname = request_json['name']
        pw = request_json['password']
        uaddress = request_json['address']
        uemail = request_json['email']

        pid = add_users(conn, uname, pw, uemail, uaddress)

        retjson['status'] = "successfully added"
        retjson['id'] = pid

        return json.dumps(retjson)

    if action == "addevent" :
        userid = request_json['userid']
        name = request_json['name']
        location = request_json['location']
        starttime = request_json['starttime']
        ts = request_json['ts']

        tid = add_events(conn, userid, name, location, starttime, ts)

        # tid = add_tasks(conn, userid, cost, price, placeid, items, tname)

        retjson['status'] = "successfully added"
        retjson['id'] = tid

        return json.dumps(retjson)

    if action == "addalarm" :
        userid = request_json['userid']
        name = request_json['name']
        location = request_json['location']
        etime = request_json['etime']
        atime = request_json['atime']

        tid = add_alarms(conn, userid, name, location, etime, atime)

        # tid = add_tasks(conn, userid, cost, price, placeid, items, tname)

        retjson['status'] = "successfully added"
        retjson['id'] = tid

        return json.dumps(retjson)


    if action == "addpreptime" :
        userid = request_json['userid']
        time = request_json['time']

        tid = add_preptime(conn, userid, time)

        # tid = add_tasks(conn, userid, cost, price, placeid, items, tname)

        retjson['status'] = "successfully added"
        retjson['id'] = tid
        retjson['time'] = time

        return json.dumps(retjson)


    if action == "addvitals" :
        userid = request_json['userid']
        pulse = request_json['pulse']
        spo2 = request_json['spo2']
        ts = request_json['ts']

        tid = add_vitals(conn, userid, pulse, spo2, ts)

        # tid = add_tasks(conn, userid, cost, price, placeid, items, tname)

        retjson['status'] = "successfully added"
        retjson['id'] = tid

        return json.dumps(retjson)


    if action == "addtherm" :
        userid = request_json['userid']
        temp = request_json['temp']

        tid = add_thermostat(conn, userid, temp)

        # tid = add_tasks(conn, userid, cost, price, placeid, items, tname)

        retjson['status'] = "successfully added"
        retjson['id'] = tid

        return json.dumps(retjson)



    if action == "createplace" :
        pname = request_json['name']
        ptype = request_json['type']
        paddress = request_json['address']
        lat = request_json['lat']
        lon = request_json['lon']
        image = request_json['image']


        pid = add_places(conn, pname, paddress, lat, lon, ptype, image)

        retjson['status'] = "successfully added"
        retjson['id'] = pid

        return json.dumps(retjson)
    
    if action == "getevents" :
        userid = request_json['userid']

        events = getevents(conn, userid)
        
        retjson['status'] = "successfully retrieved"
        retjson['events'] = events

        return json.dumps(retjson)

    if action == "getalarms" :
        userid = request_json['userid']

        events = getalarms(conn, userid)
        
        retjson['status'] = "successfully retrieved"
        retjson['alarms'] = alarms

        return json.dumps(retjson)


    if action == "gettemp" :
        userid = request_json['userid']

        events = gettemp(conn, userid)
        
        retjson['status'] = "successfully retrieved"
        retjson['thermostat'] = events

        return json.dumps(retjson)



    if action == "gettasksbyhelpee" :
        userid = request_json['userid']
        tasks = gettasks(conn, userid)
        
        retjson['status'] = "successfully retrieved"
        retjson['tasks'] = tasks

        return json.dumps(retjson)



    if action == "pickuptask" :
        helperid = request_json['helperid']
        taskid = request_json['taskid']
        
        res = pendingtask(conn, helperid, taskid)
        retjson['status'] = "task successfully picked up"

        return json.dumps(retjson)
    
    if action == "accepttask" :
        taskid = request_json['taskid']

        res = acceptedtask(conn, taskid)
        
        retjson['status'] = "task successfully accepted"

        return json.dumps(retjson)   


    if action == "completetask" :
        taskid = request_json['taskid']

        res = completedtask(conn, taskid)
        
        retjson['status'] = "task successfully accepted"

        return json.dumps(retjson)    

    

    if action == 'login':
        uemail = request_json['email']
        pw = request_json['password']

        res = login(conn, uemail, pw)

        retjson['status'] = str(res[0])
        retjson['id'] = str(res[1])
        retjson['type'] = str(res[2])
        retjson['name'] = str(res[3])
        retjson['lat'] = str(res[4])
        retjson['lon'] = str(res[5])
        retjson['address'] = str(res[6])
        

        return json.dumps(retjson)



    if action == 'getuserbyid':
        uid = request_json['uid']

        res = getuserbyid(conn, uid)

        retjson['status'] = str(res[0])
        retjson['id'] = str(res[1])
        retjson['email'] = str(res[2])
        retjson['type'] = str(res[3])
        retjson['name'] = str(res[4])
        retjson['lat'] = str(res[5])
        retjson['lon'] = str(res[6])
        retjson['address'] = str(res[7])
        

        return json.dumps(retjson)


    retstr = "action not done"

    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return retstr
