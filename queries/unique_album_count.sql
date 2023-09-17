SELECT ls.artist_name, ua.album_count AS unique_album_count
FROM liked_songs ls
JOIN (
    SELECT artist_name, COUNT(DISTINCT album_name) AS album_count
    FROM liked_songs
    GROUP BY artist_name
) ua ON ls.artist_name = ua.artist_name
GROUP BY artist_name
ORDER BY unique_album_count DESC, ls.artist_name;
