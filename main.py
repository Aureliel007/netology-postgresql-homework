import psycopg2 as pg
from psycopg2 import sql
import json

def create_db(conn: pg.extensions.connection):  
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            name VARCHAR(30) NOT NULL,
            surname VARCHAR(50) NOT NULL,
            email VARCHAR(40) UNIQUE NOT NULL
        );
        """)
        conn.commit()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones (
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES clients(id),
            phone VARCHAR(20) UNIQUE NOT NULL
        );
        """)
        conn.commit()

def add_client(conn: pg.extensions.connection,
    name: str,
    surname: str,
    email: str,
    *phones):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients (name, surname, email) VALUES(%s, %s, %s) RETURNING id;
        """, (name, surname, email))
        client_id = cur.fetchone()[0]
        
        if phones is not None:
            for phone in phones:
                add_phone(conn, client_id, phone)

def add_phone(conn: pg.extensions.connection, client_id: int, phone: str):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phones (client_id, phone) VALUES(%s, %s);
        """, (client_id, phone))
        conn.commit()

def change_client(conn: pg.extensions.connection,
    client_id: int,
    name: str = None,
    surname: str = None,
    email: str = None,
    *phones):
    client_info = {'name': name, 'surname': surname, 'email': email}
    with conn.cursor() as cur:
        for key, value in client_info.items():
            if value is not None:
                cur.execute(
                    sql.SQL("""
                UPDATE clients
                SET {}=%s
                WHERE id=%s;
                """).format(sql.Identifier(key)), (value, client_id))
                conn.commit()

        if phones is not None:
            cur.execute("""
            DELETE FROM phones WHERE client_id=%s;
            """, (client_id,))
            conn.commit()
            for phone in phones:
                add_phone(conn, client_id, phone)

def del_phone(conn: pg.extensions.connection, client_id: int, phone: str):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
        conn.commit()

def del_client(conn: pg.extensions.connection, client_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id=%s;
        """, (client_id,))
        conn.commit()
        cur.execute("""
        DELETE FROM clients WHERE id=%s;
        """, (client_id,))
        conn.commit()

def find_client(conn: pg.extensions.connection, **client_info):

    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
        SELECT c.id, c.name, c.surname, c.email, p.phone FROM clients c
        LEFT JOIN phones p ON p.client_id = c.id 
        WHERE ({}) = ({});
        """).format(
            sql.SQL(', ').join(sql.Identifier(key) for key in client_info.keys()), 
            sql.SQL(', ').join(sql.Placeholder() * len(client_info))), [val for val in client_info.values()])
        return cur.fetchall()

def load_config():
    with open('config.json', 'r') as config_file:
        return json.load(config_file)

def connect_to_db(config):
    try:
        with pg.connect(**config) as conn:
            print("Подключение к БД установлено")
            # Вызов функций тут
            create_db(conn)
            clients = [
                ('Brad', 'Pitt', 'pitt111@gmail.com'),
                ('Cillian', 'Murphy', 'cil@gmail.com', '+78945612223', '+78945612224'),
                ('Angelina', 'Jolie', 'jolie@gmail.com', '+79554443322'),
                ('Amy', 'Lee', 'amy@gmail.com'),
                ('Chris', 'Metzen', 'metzen@gmail.com', '+19995553322', '+19995553311')
            ]
            for client in clients:
                add_client(conn, *client)
            add_phone(conn, find_client(conn, name='Brad', surname='Pitt', email='pitt111@gmail.com')[0][0], '+77777777777')
            client_id = (find_client(conn, name='Chris', surname='Metzen', email='metzen@gmail.com'))[0][0]
            change_client(conn, client_id, 'Corey', 'Taylor', 'taylor@gmail.com', '+79995552222', '+79995553333')
            client_id = (find_client(conn, name='Cillian', surname='Murphy', email='cil@gmail.com'))[0][0]
            del_phone(conn, client_id, phone='+78945612223')
            del_client(conn, client_id)
            print(find_client(conn, name='Brad', surname='Pitt', email='pitt111@gmail.com'))
    except Exception as e:
        print("Нет соединения с БД:", e)
    finally:
        if conn:
            conn.close()

connect_to_db(load_config())