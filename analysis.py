import configparser
import psycopg2
from sql_queries import  count_user_query, count_artist_query, count_song_query, number_songplay_artist_query, number_songplay_user_query, number_songplay_sogn_query

def count_user(cur, conn):
    print('Number of User')
    for query in count_user_query:
        cur.execute(query)
        for row in cur:
            print(row)
       
        
def count_artist(cur, conn):
    print('Number of Artist')
    for query in count_artist_query:
        cur.execute(query)
        for row in cur:
            print(row)
        

def count_song(cur, conn):
    print('Number of Song')    
    for query in count_song_query:
        cur.execute(query)
        for row in cur:
            print(row)
            
def number_of_songplay_by_artist(cur, conn):
    print('List of the first 10 most play artists')
    print('(ID, Count)')
    for query in number_songplay_artist_query:
        cur.execute(query)
        for row in cur:
            print(row)
            
def number_of_songplay_by_song(cur, conn):
    print('List of the first 10 most listen songs')
    print('(ID, Count)')
    for query in number_songplay_sogn_query:
        cur.execute(query)
        for row in cur:
            print(row)
            
def number_of_songplay_by_user(cur, conn):
    print('List of the first 10 users order by number of songplay')
    print('(ID, Count)')
    for query in number_songplay_user_query:
        cur.execute(query)
        for row in cur:
            print(row)
        

def main():
#Extract songs metadata and user activity data from S3, transform it using a staging table, and load it into dimensional tables for analysis    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(conn)
    count_user(cur, conn)
    count_artist(cur, conn)
    count_song(cur, conn)
    
    number_of_songplay_by_artist(cur, conn)
    number_of_songplay_by_song(cur, conn)
    number_of_songplay_by_user(cur, conn)

    conn.close()
    print('Analysis successfully terminated')
   


if __name__ == "__main__":
    main()