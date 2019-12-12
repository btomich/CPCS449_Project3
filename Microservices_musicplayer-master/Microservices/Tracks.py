#README
#Tracks methods

#To create a track use the URL http://127.0.0.1:9000/recources/tracks
#Arguments- in json format - track_title: newTitle, album_title: newAlbumTitle, artist:newArtist, track_length:newLength, URL_media:newMedia (URL_media not required)

#To retrieve a track use use URL GET http://127.0.0.1:9000/recources/tracks/<GUID:id>
#Arguments - in URL -GUID  representing the ID of a given track

#To edit a Track use URL PUT http://127.0.0.1:9000/recources/tracks/<GUID:id>
#Arguments - in URL - an integer representing the ID of a given track
#Arguments - in json format - track_title:newTrackTitle

#To Delete a Track use URL DELETE http://127.0.0.1:9000/recources/tracks/<GUID:id>
#Arguments - in URL - an GUID representing the ID of a given track

import flask_api
from flask import request, jsonify,_app_ctx_stack
from flask_api import exceptions, status
import os
import time
from sqlite3 import dbapi2 as sqlite3
import uuid
import json
from flask_cassandra import CassandraCluster
from cassandra.cluster import Cluster

app = flask_api.FlaskAPI(__name__)
app.config["DEBUG"] = True
app.config.from_envvar('APP_CONFIG')
app.config['CASSANDRA_NODES'] = '172.17.0.2'

SECRET_KEY = b'_5#y2L"F4Q8z\n\xee]/'
# default authenticated configuration
app.config['BASIC_AUTH_USERNAME'] = 'bony'
app.config['BASIC_AUTH_PASSWORD'] = 'bony123'

"""We need to run this command to initialize database connection. This inturn calls get_db and create all 3 databases if not created"""
@app.cli.command('init')
def init_db():

    cassandra = CassandraCluster()
    session = cassandra.connect()

    session.execute("""CREATE  KEYSPACE IF NOT EXISTS music_player
                       WITH REPLICATION = {
                       'class' : 'SimpleStrategy', 'replication_factor' : 1 } """)

    session.set_keyspace('music_player')

    session.execute("DROP TABLE IF EXISTS tracks")
    session.execute("""CREATE TABLE IF NOT EXISTS tracks(
                    id int PRIMARY KEY,
                    artist text,
                    track_title text,
                    album_title text,
                    track_length text,
                    URL_media text,
                    URL_artwork text)""")

    session.execute("""INSERT INTO
            tracks (id, artist,track_title,album_title,track_length,URL_media,URL_artwork)
            VALUES (%(id)s, %(artist)s,%(track_title)s,%(album_title)s,%(track_length)s,%(URL_media)s,%(URL_artwork)s)
            """,
            {'id' : 1,'artist': "Aleckie", 'track_title' :"Shape of you", 'album_title': "shape of you", 'track_length': "2.30",'URL_media': "file1.m4a", 'URL_artwork': ""})
    session.execute("""INSERT INTO
            tracks (id, artist,track_title,album_title,track_length,URL_media,URL_artwork)
            VALUES (%(id)s, %(artist)s,%(track_title)s,%(album_title)s,%(track_length)s,%(URL_media)s,%(URL_artwork)s)
            """,
            {'id' : 2,'artist': "Aleckie", 'track_title' :"Shape of you 2", 'album_title': "shape of you 2", 'track_length': "2.30",'URL_media': "file2.mp3", 'URL_artwork': ""})
    session.execute("""INSERT INTO
            tracks (id, artist,track_title,album_title,track_length,URL_media,URL_artwork)
            VALUES (%(id)s, %(artist)s,%(track_title)s,%(album_title)s,%(track_length)s,%(URL_media)s,%(URL_artwork)s)
            """,
            {'id' : 3,'artist': "Aleckie", 'track_title' :"Shape of you 3", 'album_title': "shape of you 3", 'track_length': "2.30",'URL_media': "file3.mp3", 'URL_artwork': ""})
    session.execute("""INSERT INTO
            tracks (id, artist,track_title,album_title,track_length,URL_media,URL_artwork)
            VALUES (%(id)s, %(artist)s,%(track_title)s,%(album_title)s,%(track_length)s,%(URL_media)s,%(URL_artwork)s)
            """,
            {'id' : 4,'artist': "Aleckie", 'track_title' :"Shape of you 4", 'album_title': "shape of you 4", 'track_length': "2.30",'URL_media': "file4.mp3", 'URL_artwork': ""})

def check_the_track(URL):
    id1=0
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "SELECT id FROM tracks WHERE URL_media =?"
    command = command.replace("=?", "="+ "'" + URL + "'")
    command = command + " ALLOW FILTERING"
    cur=session.execute(command)

    return cur

@app.route("/")
def hello():
	return "<h1>A Super awesome API that will allow you to create and listen to tracks over and over!</h1>"

#need to also return URL of the newly-created object in the Location header field.
@app.route("/recources/tracks", methods=['GET','POST'])
def create_track():
       if request.method=='POST':
            required_fields = ['track_title','album_title','artist','track_length','URL_media']
            user_data = request.data
            if not all([field in user_data for field in required_fields]):
                raise exceptions.ParseError()


            if (request.data.get('track_title')=="") or (request.data.get('album_title')=="") or (request.data.get('artist')=="") or (request.data.get('track_length')=="") or (request.data.get('URL_media')==""):
                return exceptions.ParseError()

            cassandra = CassandraCluster()
            session = cassandra.connect()
            session.set_keyspace('music_player')
            id = session.execute("SELECT count(id) From tracks")

            nextID = ''.join(i for i in str(id[0]) if  i.isdigit())

            nextID = int(nextID) + 1


            idexists=check_the_track(request.data.get('URL_media'))

            if idexists:
              return {"Status":"Same track url already exists for one track!"}, status.HTTP_400_BAD_REQUEST

            test = session.execute("""INSERT INTO
                    tracks (id, artist,track_title,album_title,track_length,URL_media,URL_artwork)
                    VALUES (%(id)s, %(artist)s,%(track_title)s,%(album_title)s,%(track_length)s,%(URL_media)s,%(URL_artwork)s)
                    """,
                    {'id' : int(nextID),'artist': request.data.get('artist'), 'track_title' : request.data.get('track_title'), 'album_title': request.data.get('album_title'), 'track_length': request.data.get('track_length'),'URL_media': request.data.get('URL_media'), 'URL_artwork': request.data.get('URL_artwork')})
            if test is not "":
                return {"Status":str(id)}, status.HTTP_201_CREATED
            else:
                return {"Status":status.HTTP_400_BAD_REQUEST}, status.HTTP_400_BAD_REQUEST

       else:
           return {"Status":status.HTTP_204_NO_CONTENT}, status.HTTP_204_NO_CONTENT

#this method will simply route to modify, delete, or get and item based on its id
@app.route("/recources/tracks/<id>", methods=['GET','PUT','DELETE'])
def tracks(id):
    if request.method == 'PUT':
        return modify_track(id)
    elif request.method == 'DELETE':
        return delete_track(id)
    elif request.method == 'GET':
        return get_track(id)

#This URL with a POST will allow a user to modify a track with a given ID
#For now a user will only be able to modify the song name
#The data that we will request to modify will be in JSON format
#where the method will expect to key to be track_title or else an exception will be raised
def modify_track(id):
    if not request.data.get('track_title'):
        return {"Status":status.HTTP_409_CONFLICT}, status.HTTP_409_CONFLICT
    else:
        cassandra = CassandraCluster()
        session = cassandra.connect()
        session.set_keyspace('music_player')

        command = "UPDATE tracks SET track_title =newTitle  WHERE id =id"
        command = command.replace("=newTitle", "=" + "'" + str(request.data.get('track_title')) + "'")
        command = command.replace("=id", "=" + str(id))
        test = session.execute(command)
        if test is not "":
            return {"Status":status.HTTP_201_CREATED}, status.HTTP_201_CREATED
        else:
            return {"Status":status.HTTP_400_BAD_REQUEST}, status.HTTP_400_BAD_REQUEST

#this will delete a track based off of its id
def delete_track(id):
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "DELETE FROM tracks WHERE id =temp"
    command = command.replace("=temp", "="+str(id))

    test = session.execute(command)

    if test is not "":
        return {"Status":status.HTTP_204_NO_CONTENT}, status.HTTP_204_NO_CONTENT
    else:
        return {"Status":status.HTTP_400_BAD_REQUEST}, status.HTTP_400_BAD_REQUEST

#this method will return a playlist based off of its id
def get_track(id):
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "SELECT * FROM tracks WHERE id =temp"
    command = command.replace("=temp", "="+str(id))
    cur=session.execute(command)

    items=[]
    for row in cur:
        items.append({'id':row[0],'artist':row[2], 'track_title':row[4], 'album_title':row[1], 'track_length':row[3], 'URL_media':row[6], 'URL_artwork':row[5]})
    if items!=[]:
        return items
    else:
        raise exceptions.NotFound()
