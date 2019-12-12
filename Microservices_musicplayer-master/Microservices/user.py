
# Music Player API from "Creating Web APIs with Python and Flask"
# What's new
#  * Switched from Flask to Flask API
#    <https://www.flaskapi.org>
#  * Switched to PugSQL for database access
#    <https://pugsql.org>
#    This  microservice handles all the user specific information
#  * New API calls:
#    - GET /api/users/all -List of all users present in DB
#    - GET and DELETE /api/users/<string:id> to retrieve a specific user information and also delete a users information, input username in the url
#    - POST /api/users to create a new user, input data fields like {"username": "yyy", "full_name": "yyy zzz", "password": "xyz", "email": "xyz@abc", "homeurl":"www.google.com"}
#    - POST  /api/users/authenticate to authenticate a user, input data fields like {"username": "yyy", "password": "xyz"}
#    - PUT /api/users/changepassword to change a user's password, input data fields like {"username": "yyy", "password_old": "xyz","password_new": "xyz2"}

import sys
import flask_api
from flask import request
from flask_api import status, exceptions
import base64, hashlib, bcrypt, os, sys
#import base64, hashlib, os, sys
from flask_cassandra import CassandraCluster
from cassandra.cluster import Cluster


app = flask_api.FlaskAPI(__name__)
app.config.from_envvar('APP_CONFIG')
app.config['CASSANDRA_NODES'] = '172.17.0.2'


@app.cli.command('init')
#Initialize the database
def init_db():
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    session.execute("DROP TABLE IF EXISTS tracksdesc")
    session.execute("""CREATE TABLE IF NOT EXISTS tracksdesc(
                    trackid int PRIMARY KEY,
                    trackurl text,
                    description text,
                    username text,
                    )""")

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 1,'trackurl': "file1.m4a", 'description' :"My Favourite Track1", 'username': "Suramya"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 2,'trackurl': "file2.mp3", 'description' :"My Favourite Track1", 'username': "Suramya"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 3,'trackurl': "file3.mp3", 'description' :"My Favourite Track1", 'username': "Suramya"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 4,'trackurl': "file4.mp3", 'description' :"My Favourite Track1", 'username': "Suramya"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 5,'trackurl': "file1.m4a", 'description' :"My Favourite Track2", 'username': "Bony"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 6,'trackurl': "file2.mp3", 'description' :"My Favourite Track2", 'username': "Bony"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 7,'trackurl': "file3.mp3", 'description' :"My Favourite Track2", 'username': "Bony"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 8,'trackurl': "file4.mp3", 'description' :"My Favourite Track2", 'username': "Bony"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 9,'trackurl': "file1.m4a", 'description' :"My Favourite Track3", 'username': "Brandon"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 10,'trackurl': "file2.mp3", 'description' :"My Favourite Track3", 'username': "Brandon"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 11,'trackurl': "file3.mp3", 'description' :"My Favourite Track3", 'username': "Brandon"})

    session.execute("""INSERT INTO
            tracksdesc (trackid, trackurl,description,username)
            VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
            """,
            {'trackid' : 12,'trackurl': "file4.mp3", 'description' :"My Favourite Track3", 'username': "Brandon"})



    session.execute("DROP TABLE IF EXISTS users")
    session.execute("""CREATE TABLE IF NOT EXISTS users(
                    username text PRIMARY KEY,
                    full_name text,
                    password text,
                    email text,
                    homeurl text
                    )""")

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Suramya','full_name': "Suramya Singh", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "ssingh@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Bony','full_name': "Bony Roy", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "broy@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Conniewillis','full_name': "Connie Willis", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "Conniewillis@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Brandon','full_name': "Brandon Tomich", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "btomich@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Katewilhelm','full_name': "Kate Wilhelm", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "Katewilhelm@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Shreya','full_name': "Shreya Singh", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "shreyasingh@gmail.com", 'homeurl': "www.abc.com"})

    session.execute("""INSERT INTO
            users (username, full_name,password,email,homeurl)
            VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
            """,
            {'username' : 'Sudhir','full_name': "Sudhir Singh", 'password' :"$2b$12$DbmIZ/a5LByoJHgFItyZCeIEHz9koaXCGjwc/fLJAPp5G6jmQvg4u", 'email': "sudhirsingh@gmail.com", 'homeurl': "www.abc.com"})



    session.execute("DROP TABLE IF EXISTS playlists")
    session.execute("""CREATE TABLE IF NOT EXISTS playlists(
                    id int PRIMARY KEY,
                    playlist_title text,
                    URL_list text,
                    username text,
                    description text
                    )""")

    session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : 1,'playlist_title': "MyPlaylist", 'URL_list' :"['Track1','Track2','Track3','Track4']", 'username': "Brandon", 'description': "My Favourite Track1"})

    session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : 2,'playlist_title': "MyPlaylist", 'URL_list' :"['Track1','Track2','Track3','Track4']", 'username': "Suramya", 'description': "My Favourite Track2"})

    session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : 3,'playlist_title': "MyPlaylist", 'URL_list' :"['Track1','Track2','Track3','Track4']", 'username': "Bony", 'description': "My Favourite track3"})

    session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : 4,'playlist_title': "MyPlaylist", 'URL_list' :"['Track1','Track2','Track3','Track4']", 'username': "Shreya", 'description': "My Favourite Track4"})

    session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : 5,'playlist_title': "MyPlaylist", 'URL_list' :"['Track1','Track2','Track3','Track4']", 'username': "Sudhir", 'description': "My Favourite Track5"})

    session.execute("DROP TABLE IF EXISTS playlist_tracks")
    session.execute("""CREATE TABLE IF NOT EXISTS playlist_tracks(
                playlist_id int PRIMARY KEY,
                trackurl list<text>
                )""")

    session.execute("INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES (1,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])")
    session.execute("INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES (2,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])")
    session.execute("INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES (3,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])")
    session.execute("INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES (4,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])")
    session.execute("INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES (5,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])")

@app.route('/', methods=['GET'])
def home():
    return '''<h1>User Dataload Microservice running</h1>
<p>A prototype API for musiclist of users.</p>'''

#This method returns all the users in database
@app.route('/api/users/all', methods=['GET'])
def all_users():
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')
    command = "SELECT username,full_name,email,homeurl FROM users"

    all_users = session.execute(command)
    items = []
    for i in all_users:
        items.append({'username':i.username,'full_name':i.full_name, 'email': i.email, 'homeurl': i.homeurl})

    #return list(all_users),status.HTTP_200_OK
    return items, status.HTTP_200_OK


#Below url returns particular users information and also we can delete a users information from this
@app.route('/api/users/<string:id>', methods=['GET','DELETE'])
#below method returns a users information
def user(id):
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    if request.method == 'GET':
        command = "SELECT username,full_name,email,homeurl FROM users WHERE username =temp"
        command = command.replace("=temp", "="+ "'" + str(id) + "'")
        user = session.execute(command)
        items = []
        for i in user:
            items.append({'username':i.username,'full_name':i.full_name, 'email': i.email, 'homeurl': i.homeurl})

        if items != []:
            return items,status.HTTP_200_OK
        else:
            raise exceptions.NotFound()
    if request.method == 'DELETE':
        return delete_user(id)
#Delete a specific user
def delete_user(userid):
    if request.method == 'DELETE':
        username=userid
        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')

        command = "DELETE FROM users WHERE username =temp"
        command = command.replace("=temp", "="+ "'" + str(username) + "'")

        deleteQuery = session.execute(command)

        if deleteQuery is not "":
            return {username:" Deleted Successfully!"},status.HTTP_200_OK
        else:
            return {username:"Cannot delete user. Invalid Username!"},status.HTTP_400_BAD_REQUEST
    return {"status":status.HTTP_200_OK}

#This method handles creation of new user's
@app.route('/api/users', methods=['POST','GET'])
def users_ops():
     if request.method == 'POST':
         return create_user(request.data)
     return {"Status":status.HTTP_200_OK},status.HTTP_200_OK


#method to create new user. Also, validate the input json file format. Only one json row can be inserted at a time
def create_user(user):
    user = request.data

    required_fields = ['username','full_name','password','email']
    if not all([field in user for field in required_fields]):
        raise exceptions.ParseError()
    if (request.data.get('username')=="" ) or (request.data.get('full_name')=="") or (request.data.get('password')=="") or (request.data.get('email')=="") :
       raise exceptions.ParseError()
    try:
        username = request.data.get('username')
        full_name = request.data.get('full_name')
        password_hash=bcrypt.hashpw(base64.b64encode(hashlib.sha256(request.data.get('password').encode('utf-8')).digest()), b'$2b$12$DbmIZ/a5LByoJHgFItyZCe').decode('utf-8')
        email = request.data.get('email')
        homeurl=request.data.get('homeurl')

        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')
        session.execute("""INSERT INTO
                users (username, full_name,password,email,homeurl)
                VALUES (%(username)s, %(full_name)s,%(password)s,%(email)s,%(homeurl)s)
                """,
                {'username' : username,'full_name': full_name, 'password' :password_hash, 'email': email, 'homeurl': homeurl})

    except Exception as e:
        return { 'error': str(e) }, status.HTTP_409_CONFLICT
    return {user['username']: status.HTTP_201_CREATED},status.HTTP_201_CREATED


#authenticate a user if the supplied id and password is correct.
@app.route('/api/users/authenticate', methods=['GET','POST'])
def auth_user():
    if request.method == 'POST':
        username=request.data.get('username')
        password_hash=bcrypt.hashpw(base64.b64encode(hashlib.sha256(request.data.get('password').encode('utf-8')).digest()), b'$2b$12$DbmIZ/a5LByoJHgFItyZCe').decode('utf-8')

        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')

        command = "SELECT username FROM users WHERE username =username AND password =password"
        command = command.replace("=username", "="+ "'" + str(username) + "'")
        command = command.replace("=password", "="+ "'" + str(password_hash) + "'")
        command = command + " ALLOW FILTERING"
        valid = session.execute(command)
        list =[]
        for row in valid:
            list.append({'username':row.username})
        if list != []:
            return {username:" Authenticated Successfully!"},status.HTTP_200_OK
        else:
            return {username:"Cannot authenticate user. Invalid Username or Password!"},status.HTTP_400_BAD_REQUEST
    return {"status":status.HTTP_200_OK},status.HTTP_200_OK

#Change a users password if new and current password provided
@app.route('/api/users/changepassword', methods=['PUT','GET'])
def changepassword():
    if request.method == 'PUT':
        user = request.data
        required_fields = ['username','password_old','password_new']
        if not all([field in user for field in required_fields]):
            raise exceptions.ParseError()
        if (request.data.get('username')=="" ) or (request.data.get('password_old')=="") or (request.data.get('password_new')==""):
            raise exceptions.ParseError()
        try:
            username=request.data.get('username')
            password_hash_old=bcrypt.hashpw(base64.b64encode(hashlib.sha256(request.data.get('password_old').encode('utf-8')).digest()), b'$2b$12$DbmIZ/a5LByoJHgFItyZCe').decode('utf-8')
            password_hash=bcrypt.hashpw(base64.b64encode(hashlib.sha256(request.data.get('password_new').encode('utf-8')).digest()), b'$2b$12$DbmIZ/a5LByoJHgFItyZCe').decode('utf-8')

            cassandra = CassandraCluster()
            session = cassandra.connect()
            session.set_keyspace('music_player')
            command = "SELECT username FROM users WHERE username =username AND password =password"
            command = command.replace("=username", "="+ "'" + str(username) + "'")
            command = command.replace("=password", "="+ "'" + str(password_hash_old) + "'")
            command = command + " ALLOW FILTERING"

            valid = session.execute(command)
            list =[]
            for row in valid:
                list.append({'username':row.username})


            if list != []:

                command = "UPDATE users SET password =password WHERE username =username"
                command = command.replace("=username", "="+ "'" + str(username) + "'")
                command = command.replace("=password", "="+ "'" + str(password_hash) + "'")
                session.execute(command)
                return {username:"Password changed Successfully"},status.HTTP_200_OK
            else:
                return {username:" Cannot authenticate user. Invalid Username or Password!"},status.HTTP_400_BAD_REQUEST
        except Exception as e:
            return { 'error': str(e) }, status.HTTP_409_CONFLICT

        return user['username'], status.HTTP_201_CREATED
    return {"status":status.HTTP_200_OK},status.HTTP_200_OK
