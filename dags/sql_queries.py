
test_case_1 =   {'title':"Test case 1: Quality of songs data",
                'sql':'''select count(*) from songs where 
                    artistid is not null or title is not null or duration is not null''',
                'count_threshold': 0,
                'pass_message':"Test case 1: Quality of songs data passed with {} records",
                'fail_message':'Test case 1: Quality of songs data failed'
}

test_case_2 = {'title':'Test case 1: Quality of songplays data',
                'sql':'''select count(*) from songplays where
                        level is not null''',
                'count_threshold': 0,
                'pass_message':"Test case 2: Quality of songplays data passed with {} records",
                'fail_message':'Test case 2: Quality of songplays data failed'
}


class SqlQueries:
    songplay_table_insert = ("""
    SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid,
            events.location, 
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    qa_test_cases = [test_case_1,test_case_2]