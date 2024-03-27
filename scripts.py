import sqlite3
import csv
from create_db import create_authors_db, create_logs_db
import argparse

def fetch_comments_data(login, authors_db):

    cursor = authors_db.cursor()

    # Получение данных для comments.csv
    cursor.execute(''' 
        SELECT au.login, 
        post.header,
        au2.login,
        COUNT(*)
        FROM comment
        JOIN author au ON comment.author_id = au.id
        JOIN post ON post.id = comment.post_id
        JOIN author au2 ON post.author_id = au2.id
        WHERE au.login = ?
        GROUP BY au.login, post.header, au2.login
    ''', (login,))

    comments_data = cursor.fetchall()
    
    # Запись данных в CSV-файлы
    def write_to_csv(comments_data):
        with open('statistics/comments.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Login', 'Post Header','Author', 'Comment Count'])
            writer.writerows(comments_data)
    
    write_to_csv(comments_data)
    return comments_data


def fetch_general_data(login, logs_db, authors_db):
    cursor = authors_db.cursor()
    cursor.execute('''
                SELECT a.id
                FROM author a
                WHERE a.login = ?
                   ''', (login,))
    id = cursor.fetchall()[0][0]

    cursor = logs_db.cursor()

    # Получение данных для comments.csv
    cursor.execute(''' 
        SELECT strftime('%Y-%m-%d', logs.datetime) AS date,
        SUM(CASE WHEN event_type.name = 'login' THEN 1 ELSE 0 END) AS login,
        SUM(CASE WHEN event_type.name = 'logout' THEN 1 ELSE 0 END) AS logout,
        SUM(CASE WHEN event_type.name IN ('create_post', 'delete_post', 'comment') THEN 1 ELSE 0 END) as action 
        FROM logs  
        JOIN event_type ON logs.event_type_id = event_type.id
        WHERE logs.user_id = ?
        GROUP BY  strftime('%Y-%m-%d', logs.datetime) 
    ''', (id,))

    general_data = cursor.fetchall()

    # Запись данных в CSV-файлы
    def write_to_csv(general_data):
        with open('statistics/general.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Date', 'Login Count','Logout Count', 'Action Count'])
            writer.writerows(general_data)
    
    write_to_csv(general_data)
    return general_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-create_db", action="store_true", help="Создать базу данных")
    args = parser.parse_args()

    create_logs_db()
    create_authors_db()

    if args.create_db:
        print('done')
        exit()
 
    authors_db = sqlite3.connect('db/authors_database.db')
    logs_db = sqlite3.connect('db/logs_database.db')

    login = input("Enter user login: ")

    comments_data = fetch_comments_data(login, authors_db)
    general_data = fetch_general_data(login, logs_db, authors_db)

    authors_db.close()
    logs_db.close()

