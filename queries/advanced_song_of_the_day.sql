-- Select a random song that hasn't been selected as Song of the Day before
SET @random_song = (
    SELECT song_id
    FROM liked_songs
    WHERE sotd_date is null AND album_name not like '%Soundtrack%' AND album_name not like '%OST%' AND album_name not like '%TV%' AND album_name not like '%Season%' AND album_name not like '%Motion Picture%'
    ORDER BY RAND()
    LIMIT 1
);

-- Update the selected song's prev_sotd column
UPDATE liked_songs
SET sotd_date = current_date()
WHERE song_id = @random_song;

UPDATE liked_songs
SET sotd_date = null
WHERE sotd_date is not null and song_id is not null
AND NOT EXISTS (
	SELECT song_id FROM (
		SELECT song_id
        FROM liked_songs
        WHERE sotd_date is null
        ) as i
);
