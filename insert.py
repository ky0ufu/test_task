import sqlite3
import csv


db_authors = sqlite3.connect('db/authors_database.db')
cursor = db_authors.cursor()


def get_user_id(login, cursor=cursor):
    cursor.execute('''
                SELECT a.id
                FROM author a
                WHERE a.login = ?
                   ''', (login,))
    return cursor.fetchall()[0][0]


def get_blog_id(blog_name):
    cursor.execute('''
                SELECT b.id
                FROM blog b
                WHERE b.name = ?
                   ''', (blog_name,))
    return cursor.fetchall()[0][0]


def get_post_id(post_name, blog_name):
    cursor.execute('''
                SELECT p.id
                FROM post p
                JOIN blog b on b.id = p.blog_id
                WHERE p.header = ? AND b.name = ?   
                   ''', (post_name, blog_name))
    return cursor.fetchall()[0][0]


with open('test_data/author.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for row in reader:
        try:
            cursor.execute('''INSERT INTO author (login, email) VALUES (?, ?)''', (row[0], row[1]))
        except:
            print(f"BAD DATA ROW: {row}")

db_authors.commit()

with open('test_data/blog.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for row in reader:
        try:
            cursor.execute('''INSERT INTO blog (owner_id, name, description) VALUES (?, ?, ?)''', (get_user_id(row[0]), row[1], row[2]))
        except:
            print(f"BAD DATA ROW: {row}")

db_authors.commit()

with open('test_data/post.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for row in reader:
        try:
            cursor.execute('''INSERT INTO post (header, text, author_id, blog_id) VALUES (?, ?, ?, ?)''', 
                           (row[0], row[1], get_user_id(row[2]), get_blog_id(row[3])))
        except:
            print(f"BAD DATA ROW: {row}")

db_authors.commit()

with open('test_data/comment.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for row in reader:
        try:
            cursor.execute('''INSERT INTO comment (author_id, text, post_id) VALUES (?, ?, ?)''', 
                           (get_user_id(row[0]), row[1], get_post_id(row[2], row[3])))
        except:
              print(f"BAD DATA ROW: {row}")

db_authors.commit()


event_types = [
            ('login',),
            ('comment',),
            ('create_post',),
            ('delete_post',),
            ('logout',)
        ]

space_types = [
            ('global',), 
            ('blog', ), 
            ('post',)
        ]

db_logs = sqlite3.connect('db/logs_database.db')
cursor_logs = db_logs.cursor()


def get_event_type_id(event_type):
    cursor_logs.execute('''
            SELECT id
            FROM event_type
            WHERE name = ?
                   ''', (event_type,))
    
    return cursor_logs.fetchall()[0][0]


with open('test_data/logs.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for row in reader:
        try:
            cursor_logs.execute('''INSERT INTO logs (datetime, user_id, event_type_id) VALUES (?, ?, ?)''', 
                (row[0], get_user_id(row[1]), get_event_type_id(row[2])))
        except:
            print(f"BAD DATA ROW: {row}")


db_logs.commit()


db_logs.close()
db_authors.close()