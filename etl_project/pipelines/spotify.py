from dotenv import load_dotenv
import pandas as pd
import pandas as pd
import os
from etl_project.assets.assets import extract_categories, extract_new_releases 
from etl_project.assets.assets import extract_search_for_artist, extract_songs_by_artist, extract_album_tracks
from etl_project.assets.assets import extract_audio_features, extract_track, transform_album_info
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.assets import extract_categories, extract_new_releases 
from etl_project.assets.assets import extract_search_for_artist, extract_songs_by_artist, extract_album_tracks
from etl_project.assets.assets import extract_audio_features, extract_track, transform_album_info
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

if __name__=='__main__':
    # load environmment variables
    load_dotenv()
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")


    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)
    
    df_categories = extract_categories(spotify_api_client)
    

    df_new_releases=extract_new_releases(spotify_api_client)
    

    result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    artist_id = result["id"]
    songs = extract_songs_by_artist(spotify_api_client, artist_id)

    for idx, song in enumerate(songs):
        print(f"{idx + 1}. {song['name']}")