import sqlite3

def create_authors_db():
    db = sqlite3.connect('db/authors_database.db')

    cursor = db.cursor()

    # Создаем таблицу author (В схеме название таблицы users, а в describtion.txt - author)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS author (
        id INTEGER PRIMARY KEY,
        login TEXT,
        email TEXT UNIQUE
    )
    ''')

    # Создаем таблицу blog
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS blog (
        id INTEGER PRIMARY KEY,
        owner_id INTEGER,
        name TEXT,
        description TEXT,
        FOREIGN KEY(owner_id) REFERENCES users(id)
    )
    ''')

    # Создаем таблицу post
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS post (
        id INTEGER PRIMARY KEY,
        header TEXT,
        text TEXT,
        author_id INTEGER,
        blog_id INTEGER,
        FOREIGN KEY(author_id) REFERENCES users(id),
        FOREIGN KEY(blog_id) REFERENCES blog(id)
    )
    ''')

    # Создаем таблицу comment (см. problem.txt)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comment (
        id INTEGER PRIMARY KEY,
        text TEXT,
        author_id INTEGER,
        post_id INTEGER,
        FOREIGN KEY(author_id) REFERENCES users(id),
        FOREIGN KEY(post_id) REFERENCES post(id)
    )
    ''')

    db.commit()
    db.close()
    return db


def create_logs_db():
    db = sqlite3.connect('db/logs_database.db')

    cursor = db.cursor()

    # Создаем таблицу space_type
    if not check_table_existence(cursor, 'space_type'):
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS space_type (
            id INTEGER PRIMARY KEY,
            name TEXT CHECK(name IN ('global', 'post', 'blog'))
        )
        ''')

        space_types = [
            ('global',), 
            ('blog', ), 
            ('post',)
        ]
        cursor.executemany('INSERT INTO space_type (name) VALUES (?)', space_types)

    # Создаем таблицу event_type
    if not check_table_existence(cursor, 'event_type'):
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_type (
            id INTEGER PRIMARY KEY,
            name TEXT CHECK(name IN ('login', 'comment', 'create_post', 'delete_post', 'logout'))
        )
        ''')

        event_types = [
            ('login',),
            ('comment',),
            ('create_post',),
            ('delete_post',),
            ('logout',)
        ]

        cursor.executemany('INSERT INTO event_type (name) VALUES (?)', event_types)

    # Создаем таблицу logs:
    cursor.execute(''' 
    CREATE TABLE IF NOT EXISTS logs ( 
        id INTEGER PRIMARY KEY, 
        datetime TEXT, 
        user_id INTEGER, 
        space_type_id INTEGER, 
        event_type_id INTEGER, 
        FOREIGN KEY(space_type_id) REFERENCES space_type(id), 
        FOREIGN KEY(event_type_id) REFERENCES event_type(id)
    ) 
    ''')

    # Добавим триггер на автозаполнение space_type_id
    cursor.execute('''CREATE TRIGGER IF NOT EXISTS fill_space_type_id
                    AFTER INSERT ON logs
                    BEGIN
                        UPDATE logs
                        SET space_type_id = 
                            CASE 
                                WHEN (SELECT name FROM event_type WHERE id = NEW.event_type_id) = 'login' OR 
                                     (SELECT name FROM event_type WHERE id = NEW.event_type_id) = 'logout' THEN 
                                     (SELECT id FROM space_type WHERE name = 'global')
                                WHEN (SELECT name FROM event_type WHERE id = NEW.event_type_id) = 'create_post' OR 
                                     (SELECT name FROM event_type WHERE id = NEW.event_type_id) = 'delete_post' THEN 
                                     (SELECT id FROM space_type WHERE name = 'blog')
                                WHEN (SELECT name FROM event_type WHERE id = NEW.event_type_id) = 'comment' THEN 
                                     (SELECT id FROM space_type WHERE name = 'post')
                            END
                        WHERE id = NEW.id;
                    END;
                ''')
    db.commit()

    db.close()
    return db

# Функция проверки таблицы на существование. Если таблица существует то код создания таблицы не будет исполнен
def check_table_existence(cursor, table_name):
    cursor.execute('''
    SELECT name 
    FROM sqlite_master 
    WHERE type='table' AND name=?
    ''', (table_name,))

    return cursor.fetchone() is not None
