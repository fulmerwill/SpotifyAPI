SELECT artist_name, album_name, COUNT(song_title) as num_songs_saved_on_album
FROM liked_songs
group by artist_name, album_name
ORDER BY num_songs_saved_on_album DESC