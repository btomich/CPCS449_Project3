#README

#Playlist Methods

#To create a playlist use the URL POST http://127.0.0.1:5000/recources/playlists
#Arguments - in json format - playlist_title:newPlaylistTitle, URL_list:newURLList, username:newUsername, description:newDescription (description not required)

#To retrieve a playlist use use URL GET http://127.0.0.1:5000/recources/playlists/<int:id>
#Arguments - in URL - an integer representing the ID of a given playlist

#To Delete a Track use URL DELETE http://127.0.0.1:5000/recources/playlists/<int:id>
#Arguments - in URL - an integer representing the ID of a given playlist

#To retrieve all playlists use URL GET http://127.0.0.1:5000/recources/playlists/all
#Arguments - none

#To retrieve all playlists of a given user use URL GET http://127.0.0.1:5000/recources/playlists/byUser/<string:username>
#Arguments - in URL - username

import flask_api
from flask import request
from flask import jsonify
from sqlite3 import dbapi2 as sqlite3
from flask_api import exceptions, status
from flask_cassandra import CassandraCluster
from cassandra.cluster import Cluster

app = flask_api.FlaskAPI(__name__)
app.config["DEBUG"] = True
app.config.from_envvar('APP_CONFIG')
app.config['CASSANDRA_NODES'] = '172.17.0.2'


@app.route("/")
def hello():
	return "<h1>A Super awesome API that will allow you to create and listen to tracks/playlists over and over!</h1>"

@app.route("/recources/playlists/<string:id>", methods=['GET','DELETE'])
def playlists(id):
    if request.method == 'DELETE':
        return delete_playlist(id)
    elif request.method == 'GET':
        return get_playlist(id)

#this method will return a playlist based off of its id
def get_playlist(id):

    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "SELECT * FROM playlists WHERE id =id"
    command = command.replace("=id", "="+ str(id))
    command = command + " ALLOW FILTERING"

    listPlaylist = []
    current_playlist = session.execute(command)
    for row in current_playlist:
		listPlaylist.append({'id':row[0],'playlist_title':row[2], 'URL_list':row[3], 'username':row[4], 'description':row[1]})

    command = "SELECT * FROM playlist_tracks WHERE playlist_id =playlist_id"
    command = command.replace("=playlist_id", "="+ str(id))
    command = command + " ALLOW FILTERING"
    track_lists = session.execute(command)

    if len(list(listPlaylist)) != 0:

        track_list = []
        for tracks in track_lists[0][1]:
            track_list.append({'playlist_id': id,'trackurl':tracks})
        listPlaylist.append(track_list)
        return listPlaylist, status.HTTP_200_OK

    else:
        return  {"Status":"Playlist ID does not exists in database! Please check with the existing ID's 1,2,3,4,5"}, status.HTTP_400_BAD_REQUEST


#this method will delete a playlist based off of its id
def delete_playlist(id):

    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')

    command = "DELETE FROM playlists WHERE id =id"
    command = command.replace("=id", "="+ str(id))
    delete_cmd = session.execute(command)

    if len(list(delete_cmd)) != 0:
        return {"Status":status.HTTP_204_NO_CONTENT}, status.HTTP_204_NO_CONTENT
    else:
        return {"Status":status.HTTP_400_BAD_REQUEST}, status.HTTP_400_BAD_REQUEST

#this method will create a playlist
@app.route("/recources/playlists", methods=['GET','POST'])
def create_playlist():
    required_fields = ['playlist_title','URL_list','username']
    user_data = request.data
    if not all([field in user_data for field in required_fields]):
        raise exceptions.ParseError()
    if (request.data.get('playlist_title')=='') or (request.data.get('URL_list')=='') or (request.data.get('username')==''):
        raise exceptions.ParseError()

    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')
    command = "SELECT username,full_name,email,homeurl FROM users WHERE username =id"
    command = command.replace("=id", "="+ "'" + str(request.data.get('username')) + "'")
    command = command + " ALLOW FILTERING"
    user = session.execute(command)
    if len(list(user)) != 0:
        pass
    else:
        return  {"Status":"User does not exists in user table!"}, status.HTTP_400_BAD_REQUEST
    getNewId = session.execute("SELECT count(id) From playlists")
    getNewId = ''.join(i for i in str(getNewId[0]) if  i.isdigit())
    getNewId = int(getNewId) + 1
    flag = 1
    while flag == 1:
        getNewId = getNewId + 1
        command = "SELECT username FROM playlists WHERE id =id"
        command = command.replace("=id", "="+ str(getNewId))
        command = command + " ALLOW FILTERING"
        user = session.execute(command)
        if len(list(user)) == 0:
            flag = 0
    final = session.execute("""INSERT INTO
                playlists (id, playlist_title,URL_list,username,description)
                VALUES (%(id)s, %(playlist_title)s,%(URL_list)s,%(username)s,%(description)s)
                """,
                {'id' : getNewId,'playlist_title' : request.data.get('playlist_title'), 'URL_list' : str(request.data.get('URL_list')), 'username' : request.data.get('username'), 'description' : request.data.get('description')})
    command = "INSERT INTO playlist_tracks (playlist_id, trackurl) VALUES ( getNewId ,['http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbadf','http://localhost:8000/tracks/805944b7-b211-4e26-b74e-6c75d92cbabc','http://localhost:8000/tracks/9944dd14-499a-4d64-8a78-62cc35205449','http://localhost:8000/tracks/1000dd14-499a-4d64-8a78-62cc35205455'])"
    command = command.replace("getNewId", str(getNewId))
    final2 = session.execute(command)

    if len(list(final)) == 0 and len(list(final2)) == 0:
        return {"Created ID is: ": str(getNewId)}, status.HTTP_201_CREATED
    else:
        return {"Status":status.HTTP_400_BAD_REQUEST}, status.HTTP_400_BAD_REQUEST

#this method will pull all the data from the database for given user
#since this query returns an iterable we need to convert it to a list to return
#if the list is empty a 404 is returned else we return a list containing
#all the rows that match the user name
@app.route("/recources/playlists/byUser/<string:username>", methods=['GET'])
def get_user_playlist(username):
    cassandra = CassandraCluster()
    session = cassandra.connect()
    session.set_keyspace('music_player')
    command = "SELECT * FROM playlists WHERE username =username"
    command = command.replace("=username", "="+ "'" + str(username) + "'")
    command = command + " ALLOW FILTERING"

    final =[]
    user_playlists = session.execute(command)
    for row in user_playlists:
       final.append({'id':row.id,'description':row.description, 'playlist_title':row.playlist_title, 'url_list':row.url_list, 'username':row.username})
    if final != []:
       return final, status.HTTP_200_OK
    else:
        return {"Status":status.HTTP_404_NOT_FOUND}, status.HTTP_404_NOT_FOUND
