SELECT *
FROM liked_songs
WHERE prev_sotd is false
ORDER BY RAND()
LIMIT 1;