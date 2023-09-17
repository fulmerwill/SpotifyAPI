SELECT artist_name, COUNT(song_title) as num_songs_saved
FROM liked_songs
group by artist_name
ORDER BY num_songs_saved DESC