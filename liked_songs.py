import os
import spotipy
import sqlalchemy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine, text, exc

# Connect to the database
load_dotenv()
choice = int(input('Insert 0 for local database, insert 1 for heroku database.'))
if choice == 0:
    DB_NAME = os.environ.get('LOCAL_NAME')
    DB_USER = os.environ.get('LOCAL_USER')
    DB_PASS = os.environ.get('LOCAL_PASS')
    DB_HOST = os.environ.get('LOCAL_HOST')
    engine_string = f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(engine_string)
    table_name = 'liked_songs'
else:
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_HOST = os.environ.get('DB_HOST')
    engine_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(engine_string)
    table_name = 'liked_songs'

scope = "user-library-read"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope), requests_timeout=20)

liked_songs = []
cols = ['project_id', 'order_num', 'song_id', 'song_title', 'playback', 'artist_id', 'artist_name', 'album_id',
        'album_name', 'album_cover', 'duration_min']


def get_saved_tracks():
    print('Calling the Spotify API...')

    offset = 0
    limit = 50
    order_id = 1
    while True:
        saved_tracks = sp.current_user_saved_tracks(limit=limit, offset=offset)
        if not saved_tracks['items']:
            break
        for saved_track in saved_tracks['items']:
            duration_ms = saved_track['track']['duration_ms']
            duration_min = (duration_ms / 1000) / 60
            duration_min = round(duration_min, 2)
            album_uri = saved_track['track']['album']['uri']
            album_id = album_uri.replace('spotify:album:', '')
            liked_songs.append({'project_id': 'P006',
                                'order_num': order_id,
                                'song_id': saved_track['track']['id'],
                                'song_title': saved_track['track']['name'],
                                'playback': saved_track['track']['external_urls']['spotify'],
                                'artist_id': saved_track['track']['artists'][0]['id'],
                                'artist_name': saved_track['track']['artists'][0]['name'],
                                'album_id': album_id,
                                'album_name': saved_track['track']['album']['name'],
                                'album_cover': saved_track['track']['album']['images'][0]['url'],
                                'duration_min': duration_min,
                                })
            order_id += 1

        offset += limit


def check_table_exists():
    with engine.connect() as connection:
        table_exists_query = "SELECT EXISTS (" \
                             "SELECT 1 " \
                             "FROM information_schema.tables " \
                             "WHERE table_name = :table " \
                             ");"
        table_exists = connection.execute(text(table_exists_query), {'table': table_name}).fetchone()[0]

    return table_exists


def insert_songs_into_database(song):
    table_exists = check_table_exists()

    with engine.connect() as connection:
        song_columns = {'order_num': song['order_num'], 'song_id': song['song_id'],
                        'song_title': song['song_title'], 'playback': song['playback'], 'artist_id': song['artist_id'],
                        'artist_name': song['artist_name'], 'album_id': song['album_id'],
                        'album_name': song['album_name'], 'album_cover': song['album_cover'],
                        'duration_min': song['duration_min']
                        }
        if table_exists:
            print('Adding songs into the database!')
            try:
                add_songs = f"INSERT INTO {table_name} (order_num, song_id, song_title, playback, artist_id, " \
                            "artist_name, album_id, album_name, album_cover, duration_min) " \
                            "VALUES (:order_num, :song_id, :song_title, :playback, :artist_id, :artist_name, " \
                            ":album_id, :album_name, :album_cover, :duration_min)"

                connection.execute(
                    text(add_songs),
                    song_columns
                )
                connection.commit()
            except sqlalchemy.exc.IntegrityError:
                print('The database is currently up to date!')
                quit()

        else:
            print('Creating liked songs table.')

            create_table = f"CREATE TABLE {table_name} (" \
                           "project_id VARCHAR(100) NOT NULL DEFAULT 'P001', " \
                           "order_num BIGINT NOT NULL, " \
                           "song_id VARCHAR(500) NOT NULL, " \
                           "song_title VARCHAR(250) NOT NULL," \
                           "playback VARCHAR(500) NOT NULL UNIQUE, " \
                           "artist_id VARCHAR(500) NOT NULL, " \
                           "artist_name VARCHAR(250) NOT NULL, " \
                           "album_id VARCHAR(500) NOT NULL, " \
                           "album_name VARCHAR(250) NOT NULL, " \
                           "album_cover VARCHAR(500) NOT NULL, " \
                           "duration_min NUMERIC(5, 2), " \
                           "sotd_date DATE UNIQUE, " \
                           "PRIMARY KEY (song_id), " \
                           "FOREIGN KEY (project_id) REFERENCES projects(project_id)" \
                           ")"
            connection.execute(text(create_table))
            connection.commit()


def get_existing_song_ids():
    with engine.connect() as connection:
        try:
            ids_query = f"SELECT song_id FROM {table_name};"
            result = connection.execute(text(ids_query))
            existing_song_ids = {row[0] for row in result}
        except sqlalchemy.exc.ProgrammingError:
            print('Liked Songs table does not exist yet.')
            existing_song_ids = {0 for song in liked_songs}

    return existing_song_ids


def remove_songs():
    existing_song_ids = get_existing_song_ids()
    removed = []

    print('Attempting to remove songs from database...')

    if len(removed) == 0:
        print('No songs to remove!')

    else:
        print('Songs to Remove:')
        for idx in existing_song_ids:
            if idx not in [song['song_id'] for song in liked_songs]:
                with engine.connect() as connection:
                    remove_query = f"DELETE FROM {table_name} " \
                                   "WHERE song_id = :idx;"
                    connection.execute(text(remove_query), {'idx': idx})
                    connection.commit()

                removed.append(idx)
                print(idx)

    return removed


def filter_new_songs():
    print('Checking if new songs have been added to the library...')

    existing_song_ids = get_existing_song_ids()
    new_songs = [song for song in liked_songs if song['song_id'] not in existing_song_ids]

    if len(new_songs) == 0:
        print('No new songs to add.')
    return new_songs


def update_indices(new_songs):
    print('Updating song order.')
    count = {'count': len(new_songs)+1}
    with engine.connect() as connection:
        update_query = f"UPDATE {table_name} " \
                       "SET order_num = order_num + :count"
        connection.execute(text(update_query), {'count': count})
        connection.commit()


def main():
    get_saved_tracks()
    table_exists = check_table_exists()

    if table_exists:
        remove_songs()
        new_songs = filter_new_songs()

        if new_songs:
            print('Adding new songs to the database...')
            update_indices(new_songs)
            for song in new_songs:
                insert_songs_into_database(song)

    else:
        for song in liked_songs:
            insert_songs_into_database(song)

    print('The database is currently up to date!')
    print(' ')
    print('Updating song of the day...')
    with engine.connect() as connection:
        if choice == 0:
            sotd_query = "SET @random_song = ( " \
                         "SELECT song_id " \
                         "FROM liked_songs " \
                         "ORDER BY RAND() " \
                         "LIMIT 1); " \
                         "UPDATE liked_songs " \
                         "SET sotd_date = current_date() " \
                         "WHERE song_id = @random_song; "

            connection.execute(text(sotd_query))
        else:
            connection.dialect = 'PostgreSQL'
            connection.execute(text("WITH random_song_cte AS "
                                    "( SELECT song_id FROM liked_songs "
                                    "WHERE ( sotd_date IS NULL "
                                    "AND album_name NOT LIKE '%Soundtrack%' "
                                    "AND album_name NOT LIKE '%OST%' "
                                    "AND album_name NOT LIKE '%TV%' "
                                    "AND album_name NOT LIKE '%Season%' "
                                    "AND album_name NOT LIKE '%Motion Picture%' "
                                    "AND artist_name not like '%Richard Cheese%') "
                                    "ORDER BY random() "
                                    "LIMIT 1)"
                                    "UPDATE liked_songs"
                                    "SET sotd_date = current_date"
                                    "WHERE song_id = (SELECT song_id FROM random_song_cte);"))
        print('Song of the Day chosen!')


if __name__ == '__main__':
    main()
