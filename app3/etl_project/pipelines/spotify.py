from dotenv import load_dotenv
import pandas as pd
import json
import os
from etl_project.assets.assets import extract_album_track_data, extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist, extract_album_tracks, extract_audio_features, extract_track, extract_album_tracks_features, extract_album_popularity 
from etl_project.assets.assets import transform_album_info, transform_features_track_popularity, load, transform_techniques
from etl_project.assets.sql_transform import transform_sql_order, SqlTransform

from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.spotify_api import SpotifyApiClient
from graphlib import TopologicalSorter
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, TIMESTAMP, func, Date, Text, VARCHAR, engine
import yaml
import sys
from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from sqlalchemy.exc import SQLAlchemyError


if __name__=='__main__':
    # load environmment variables
    load_dotenv()

    #get config variables
    yaml_file_path =  __file__.replace(".py",".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipline_config = yaml.safe_load(yaml_file)
            config = pipline_config.get("config")
            PIPELINE_NAME = pipline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file!"
        )
    # print(config.get("log_folder_path"))
    #client for pipeline logger
    pipeline_logging = PipelineLogging(
        pipeline_name= pipline_config.get("name"),
        log_folder_path= config.get("log_folder_path")
    ) 

    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME= os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME=os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD=os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT=os.environ.get("LOGGING_PORT")

    print(LOGGING_DATABASE_NAME)

    pipeline_logging.logger.info("Setting up client for logging metadata into postgreql")
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )
    pipeline_logging.logger.info("Setting up metadata logger")
    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME,
        postgresql_client= postgresql_logging_client,
        config=config,
    )
    try:
        metadata_logger.log()
        pipeline_logging.logger.info("Starting pipeline run")
        pipeline_logging.logger.info("Getting pipeline environment variables")
        client_id=os.environ.get('CLIENT_ID')
        client_secret=os.environ.get("CLIENT_SECRET")
        numberofreleases=os.environ.get("NUMBER_OF_RELEASES")
        
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME =os.environ.get("DATABASE_NAME")
        PORT =os.environ.get("PORT")

        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            database_name=DATABASE_NAME,
            port=PORT,
        )


        pipeline_logging.logger.info("Setting up Spotify API Client")
        spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)

        pipeline_logging.logger.info("Extracting details from Spotify API")
        
        ###Extract and Load begin ###
        pipeline_logging.logger.info("********** Getting New Releases **********")
        pipeline_logging.logger.info("New releases")
        df_new_releases = extract_new_releases(spotify_api_client, numberofreleases=numberofreleases)
        df_new_releases["id"] = df_new_releases["id"].astype(str)
        
        new_releases_metadata=MetaData()
        pipeline_logging.logger.info("Pre-load metadata for new releases")
        #pipeline_logging.logger.info("Connection created")
        new_releases_table = Table(
            'new_releases', 
            new_releases_metadata,
            Column("album_name", String),
            Column('album_id', String, primary_key=True),
            Column('release_date', Date),
            Column('total_tracks', Float),
            Column('artist_name', String),
            Column('artist_id', String),
            Column('extracted_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())
        )
        postgresql_client.create_table(new_releases_metadata)

        # Check if the constraint exists
        constraint_check_sql = """
        SELECT conname 
        FROM pg_constraint 
        WHERE conname = 'unique_album_id' AND conrelid = 'new_releases'::regclass;
        """

        with postgresql_client.engine.connect() as connection:
            result = connection.execute(constraint_check_sql).fetchone()
            if result is None:
                # Constraint does not exist, so add it
                add_unique_constraint_sql = "ALTER TABLE new_releases ADD CONSTRAINT unique_album_id UNIQUE (album_id);"
                connection.execute(add_unique_constraint_sql)
            else:
                pipeline_logging.logger.info("The unique constraint 'unique_album_id' already exists.")

        new_releases_source_table_name = "new_releases"  # Replace with your actual template name

        #identify albums not extracted before
        #2 transformation techniques select certain attributes such as album name, album id, release_date, total_tracks, and etc
        #filter and rename select columns from dataframe
        select_album_info = transform_album_info(df_new_releases, new_releases_source_table_name, postgresql_client=postgresql_client)
        pipeline_logging.logger.info("transform album info")


        # If there are new albums to extract, proceed with extracting audio releases
        if isinstance(select_album_info, pd.DataFrame) and not select_album_info.empty:  
            print(select_album_info)
            pipeline_logging.logger.info("Saving new releases into database")
            postgresql_client.create_table(new_releases_metadata)
            load(
                df=select_album_info,
                postgresql_client=postgresql_client,
                table=new_releases_table,
                metadata=new_releases_metadata,
                load_method="upsert",
            )
        else:
            pipeline_logging.logger.info("No new releases info to extract, ending program gracefully")
            sys.exit(0)
            #sys.exit(0) this caused a logging error

        album_track_data = extract_album_track_data(spotify_api_client, select_album_info)
        pipeline_logging.logger.info("get features")
        df_features = extract_album_tracks_features(spotify_api_client, album_track_data)

        audio_features_metadata=MetaData()
        pipeline_logging.logger.info("load audio_features into table")
        #pipeline_logging.logger.info("Connection created")
        audio_table = Table(
            'audio_features', 
            audio_features_metadata,
            Column("danceability", Float),
            Column('energy', Float),
            Column('key', Float),
            Column('loudness', Float),
            Column('mode', Float),
            Column('speechiness', Float),
            Column('acousticness', Float),        
            Column('instrumentalness', Float),
            Column('liveness', Float),
            Column('valence', Float),
            Column('tempo', Float),
            Column('type', String),
            Column('id', String, primary_key=True),
            Column('uri', String),
            Column('track_href', String),
            Column('analysis_url', String),
            Column('duration_ms', Integer),
            Column('time_signature', Float),
            Column('extracted_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())
        )
        postgresql_client.create_table(audio_features_metadata)
        
        load(
            df=df_features,
            postgresql_client=postgresql_client,
            table=audio_table,
            metadata=audio_features_metadata,
            load_method="upsert",
        )
        # print("get track details")
        df_track_popularity = extract_album_popularity(spotify_api_client, album_track_data)

        df_track_popularity['artists'] = df_track_popularity['artists'].apply(lambda artist_list: [artist['name'] for artist in artist_list])
        df_track_popularity['album.artists'] = df_track_popularity['album.artists'].apply(lambda x: x[0]['name'] if x else None)
        #normalizing the list into json to help with increase querying capabilities
        df_track_popularity['available_markets'] = df_track_popularity['available_markets'].apply(json.dumps)
        df_track_popularity['album.available_markets'] = df_track_popularity['album.available_markets'].apply(json.dumps)


        track_details_metadata=MetaData()
        pipeline_logging.logger.info("load track details into table")
        #pipeline_logging.logger.info("Connection created")
        track_table = Table(
            'track_details', 
            track_details_metadata,
            Column("artists", String),
            Column('available_markets', String),
            Column('disc_number', Integer),
            Column('duration_ms', Integer),
            Column('explicit', String),
            Column('href', String),
            Column('id', String, primary_key=True),        
            Column('is_local', String),
            Column('name', String),
            Column('popularity', Integer),
            Column('preview_url', String),
            Column('track_number', Integer),
            Column('type', String),
            Column('uri', String),
            Column('album.album_type', String),
            Column('album.artists', String),
            Column('album.available_markets', String),
            Column('album.external_urls.spotify', String),
            Column('album.href', String),
            Column('album.id', String),
            Column('album.images', String),
            Column('album.name', String),
            Column('album.release_date', Date),
            Column('album.release_date_precision', String),
            Column('album.total_tracks', String),
            Column('album.type', String),
            Column('album.uri', String),
            Column('external_ids.isrc', String),
            Column('external_urls.spotify', String),
            Column('extracted_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())                
        )
        postgresql_client.create_table(track_details_metadata)

        load(
            df=df_track_popularity,
            postgresql_client=postgresql_client,
            table=track_table,
            metadata=track_details_metadata,
            load_method="upsert",
        )
    
        ###Extract and Load completed ###

        ### Transform Techniques ###

        transform_template_environment = Environment(
            loader=FileSystemLoader("etl_project/assets/sql/transform")
        )

        # create nodes
        Track_Features_Details_Baseline = SqlTransform(
            table_name="Track_Features_Details_Baseline",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        Ten_Most_Popular = SqlTransform(
            table_name="Ten_Most_Popular",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        Ranked_Songs_by_Album = SqlTransform(
            table_name="Ranked_Songs_by_Album",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
        Each_Album_Top_Three_Songs = SqlTransform(
            table_name="Each_Album_Top_Three_Songs",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )

        # create DAG
        dag = TopologicalSorter()
        dag.add(Track_Features_Details_Baseline)  # Create this table first

        dagA = TopologicalSorter()
        dagA.add(Ten_Most_Popular)  # Create this table first

        dagB = TopologicalSorter()
        dagB.add(Ranked_Songs_by_Album)  # Create this table first

        dagC = TopologicalSorter()
        dagC.add(Each_Album_Top_Three_Songs)  # Create this table first

        # run transform
        pipeline_logging.logger.info("Perform transform")
        transform_sql_order(dag=dag)
        transform_sql_order(dag=dagA)
        transform_sql_order(dag=dagB)
        transform_sql_order(dag=dagC)
        pipeline_logging.logger.info("Pipeline complete")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
        pipeline_logging.logger.handlers.clear()        



    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. see details logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAIL, logs=pipeline_logging.get_logs()
        )
        # sys.exit(1)  # Exit with a status code of 1 (indicating an error)
