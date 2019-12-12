#    This  microservice handles all the track description update specific information
#  * New API calls:
#    - POST /api/users/settrackdesc change description of a track, input data {  "trackurl": "track123","username": "bony2018", "description": "My favourite track1!"}
#      Note if trackurl and username combo not exists it will insert, otherwise it will update the description
#    - GET /api/users/gettrackdesc/<string:username>/<string:trackurl> to retrieve a track description of specific user , input username and trackurl in the API url


import sys
import flask_api
from flask import request,_app_ctx_stack
from flask_api import status, exceptions
#import pugsql
import base64, hashlib, bcrypt, os, sys
from sqlite3 import dbapi2 as sqlite3
from flask_cassandra import CassandraCluster
from cassandra.cluster import Cluster

app = flask_api.FlaskAPI(__name__)

app.config["DEBUG"] = True
app.config.from_envvar('APP_CONFIG')
app.config['CASSANDRA_NODES'] = '172.17.0.2'


def check_the_track(URL):
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "SELECT id FROM tracks WHERE URL_media =?"
    command = command.replace("=?", "="+ "'" + URL + "'")
    command = command + " ALLOW FILTERING"
    cur=session.execute(command)

    return cur


#Base url
@app.route('/', methods=['GET'])
def home():
    return '''<h1>Tracks description microservice running</h1>
<p>A prototype API for musiclist of users.</p>'''


#Set a tracks description if not present. Also, if description already present update it
@app.route('/api/users/settrackdesc', methods=['GET','POST'])
def track_ops():
    if request.method == 'POST':
        return create_desc(request.data)
    return {"status":status.HTTP_200_OK},status.HTTP_200_OK

#Update or create description of track
def create_desc(track):

    track = request.data
    required_fields = ['trackurl','description','username']
    if not all([field in track for field in required_fields]):
        raise exceptions.ParseError()
    try:
        username = request.data.get('username')
        trackurl = request.data.get('trackurl')
        description=request.data.get('description')


        if (trackurl==""):
            return { 'error': "Invalid trackurl!" },status.HTTP_400_BAD_REQUEST


        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')

        command = "SELECT username,full_name,email,homeurl FROM users WHERE username =id"
        command = command.replace("=id", "="+ "'" + str(username) + "'")
        command = command + " ALLOW FILTERING"
        valid = session.execute(command)
        list =[]
        for row in valid:
            list.append({'full_name':row.full_name})

        command = "SELECT trackid FROM tracksdesc WHERE username =username and trackurl =trackurl"
        command = command.replace("=username", "="+ "'" + str(username) + "'")
        command = command.replace("=trackurl", "="+ "'" + str(trackurl) + "'")
        command = command + " ALLOW FILTERING"
        trackid = session.execute(command)
        listTrackid =[]
        for row in trackid:
            idToGet = row.trackid
            listTrackid.append({'trackid':row.trackid})
        print("Hello ",trackurl,trackid)

        id_check=check_the_track(trackurl)
        idList =[]
        for row in id_check:
            idList.append({'trackid':row.id})

        if idList == []:
            return { 'error': "Track url invalid! No reference found in Track list." }, status.HTTP_400_BAD_REQUEST
        print(trackid,trackurl,username,description)
        if list != []:

                session.execute("""INSERT INTO
                        tracksdesc (trackid, trackurl,description,username)
                        VALUES (%(trackid)s, %(trackurl)s,%(description)s,%(username)s)
                        """,
                        {'trackid': int(idToGet),'trackurl': request.data.get('trackurl'), 'description': request.data.get('description'), 'username': request.data.get('username')})

                return {"Success":status.HTTP_202_ACCEPTED},status.HTTP_202_ACCEPTED

        else:
            return {track['username']:" Does not exists"},status.HTTP_409_CONFLICT
    except Exception as e:
        return { 'error': str(e) }, status.HTTP_409_CONFLICT
    return {track['trackurl']:status.HTTP_201_CREATED},status.HTTP_201_CREATED

#Get a track specific URL.
@app.route('/api/users/gettrackdesc/<string:username>/<string:trackurl>', methods=['GET'])
def track_ret(username,trackurl):


        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')

        command = "SELECT description, trackid FROM tracksdesc WHERE username =username and trackurl =trackurl"
        command = command.replace("=username", "="+ "'" + str(username) + "'")
        command = command.replace("=trackurl", "="+ "'" + str(trackurl) + "'")
        command = command + " ALLOW FILTERING"
        query = session.execute(command)

        descriptList =[]
        for row in query:
            finalDesc = row.description
            descriptList.append({'trackid':row.description})
        if descriptList != []:
            return {trackurl:finalDesc},status.HTTP_200_OK
        else:
            return {trackurl:"Do not exists"},status.HTTP_400_BAD_REQUEST
