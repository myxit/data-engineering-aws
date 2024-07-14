import configparser
import psycopg2
import matplotlib.pyplot as plt
from sql_report_queries import most_played_songs, highest_usage_time_of_day, top_10_users_by_number_played, most_popular_artists, song_plays_by_user_subscription

def build_most_played_songs(conn):
    with conn.cursor() as cur: 
        cur.execute(most_played_songs)
        records = cur.fetchall()
    
    titles = [record[0] for record in records]
    play_count = [record[1] for record in records]

    plt.bar(titles, play_count)
    plt.xlabel('Songs')
    plt.xticks(rotation=90)
    plt.ylabel('Count')
    plt.title("Most Played Songs")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.3)
    plt.savefig('reports_examples/most_played_songs.png')
    plt.clf()


def build_highest_usage_time_of_day(conn):
    with conn.cursor() as cur: 
        cur.execute(highest_usage_time_of_day)
        records = cur.fetchall()
    
    hours = [record[0] for record in records]
    play_count = [record[1] for record in records]

    plt.bar(hours, play_count)
    plt.xlabel('Hour')
    plt.ylabel('Play Count')
    plt.title("Highest Usage Time of Day")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.3)
    plt.savefig('reports_examples/highest_usage_time_of_day.png')
    plt.clf()


def build_top_10_users_by_number_played(conn):    
    with conn.cursor() as cur: 
        cur.execute(top_10_users_by_number_played)
        records = cur.fetchall()
    
    user_id = ["uid_" + str(record[0]) for record in records]
    play_count = [record[1] for record in records]

    plt.bar(user_id, play_count)
    plt.xlabel('user_id')
    plt.ylabel('Play Count')
    plt.title("Top 10 Users by Number of Songs Played")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.3)
    plt.savefig('reports_examples/top_10_users_by_number_played.png')
    plt.clf()


def build_most_popular_artists(conn):
    with conn.cursor() as cur: 
        cur.execute(most_popular_artists)
        records = cur.fetchall()
    
    artists = [record[0] for record in records]
    play_count = [record[1] for record in records]

    plt.bar(artists, play_count)
    plt.xlabel('Artists')
    plt.xticks(rotation=90)
    plt.ylabel('Count')
    plt.title("Most Popular Artists")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.3)
    plt.savefig('reports_examples/most_popular_artists.png')
    plt.clf()


def build_song_plays_by_user_subscription(conn):
    with conn.cursor() as cur: 
        cur.execute(song_plays_by_user_subscription)
        records = cur.fetchall()
    
    levels = [record[0] for record in records]
    play_count = [record[1] for record in records]

    plt.bar(levels, play_count)
    plt.xlabel('Song Plays by User Subscription Level')
    plt.ylabel('Count')
    plt.title("Subscription")
    plt.tight_layout()
    plt.savefig('reports_examples/song_plays_by_user_subscription.png')
    plt.clf()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    build_most_played_songs(conn)
    build_highest_usage_time_of_day(conn)
    build_top_10_users_by_number_played(conn)
    build_most_popular_artists(conn)
    build_song_plays_by_user_subscription(conn)

    conn.close()


if __name__ == "__main__":
    main()