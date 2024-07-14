# Most Played Songs
most_played_songs = ("""
   SELECT s.title, COUNT(sp.songplay_id) AS play_count
   FROM songplay sp
   JOIN songs s ON sp.song_id = s.song_id
   GROUP BY s.title
   ORDER BY play_count DESC
   LIMIT 10;
""")

# Highest Usage Time of Day
highest_usage_time_of_day = ("""
   SELECT t.hour, COUNT(sp.songplay_id) AS play_count
   FROM songplay sp
   JOIN time t ON sp.start_time = t.start_time
   GROUP BY t.hour
   ORDER BY t.hour ASC;
""")

# Top 10 Users by Number of Songs Played
top_10_users_by_number_played = ("""
    SELECT u.user_id, COUNT(sp.songplay_id) AS songplays
    FROM songplay sp
    JOIN users u ON sp.user_id = u.user_id
    GROUP BY u.user_id, u.first_name, u.last_name
    ORDER BY songplays DESC
    LIMIT 10;
""")

# Most Popular Artists
most_popular_artists = ("""
    SELECT a.name, COUNT(sp.songplay_id) AS play_count
    FROM songplay sp
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY a.name
    ORDER BY play_count DESC
    LIMIT 10;
""")

# Song Plays by User Subscription Level
song_plays_by_user_subscription = ("""
    SELECT u.level, COUNT(sp.songplay_id) AS play_count
    FROM songplay sp
    JOIN users u ON sp.user_id = u.user_id
    GROUP BY u.level
    ORDER BY play_count DESC;
""")
