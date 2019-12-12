#README
#Tracks methods

#To create a track use the URL http://127.0.0.1:5000/recources/tracks
#Arguments- in json format - track_title: newTitle, album_title: newAlbumTitle, artist:newArtist, track_length:newLength, URL_media:newMedia (URL_media not required)

#To retrieve a track use use URL GET http://127.0.0.1:5000/recources/tracks/<int:id>
#Arguments - in URL - an integer representing the ID of a given track

#To edit a Track use URL PUT http://127.0.0.1:5000/recources/tracks/<int:id>
#Arguments - in URL - an integer representing the ID of a given track
#Arguments - in json format - track_title:newTrackTitle

#To Delete a Track use URL DELETE http://127.0.0.1:5000/recources/tracks/<int:id>
#Arguments - in URL - an integer representing the ID of a given track

import flask_api
from flask import request, jsonify,_app_ctx_stack
from flask_api import exceptions, status
import pugsql
import requests
import os
import time
from sqlite3 import dbapi2 as sqlite3
import uuid



app = flask_api.FlaskAPI(__name__)



@app.route("/")
def hello():
    return "Welcome to XSPF conversion! Please add username into current URL to get the details."

@app.route("/<string:username>")
def homepage(username):
    print(username)
    r = requests.get(
      'http://127.0.0.1:9003/recources/playlists/'+username,params={'username': username})
    with open('playlists.tmp.json', 'w') as f:
        f.write(r.text)
    os.rename('playlists.tmp.json', 'playlists.json')
    
    return "hello"
