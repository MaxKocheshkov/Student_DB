import psycopg2 as pg
from datetime import datetime
from psycopg2.extras import DictCursor


def create_db():  # создает таблицы
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            gpa DECIMAL(10, 2),
            birth TIMESTAMP WITH TIME ZONE NOT NULL
        );''')

        cur.execute('''CREATE TABLE course (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                );''')

        cur.execute('''CREATE TABLE students_course (
                            student_id INT REFERENCES students(id),
                            course_id INT REFERENCES course(id),
                            CONSTRAINT students_course_pk PRIMARY KEY(student_id, course_id)
                        );''')


def get_students(course_id):  # возвращает студентов определенного курса
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor(cursor_factory=DictCursor)
        cur.execute("""SELECT s.id, s.name, c.name FROM students_course sc
                    join students s on s.id = sc.student_id
                    join course c on c.id = sc.course_id WHERE c.id=%s
                    """, (course_id,))
        print(cur.fetchall())


def add_students(course_id, students):
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor()
        cur.execute('''INSERT INTO students_course (student_id, course_id) VALUES (%s, %s)
        ''', (students, course_id))


def add_student(student):
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor()
        cur.execute('''INSERT INTO students(name, birth) VALUES (%s, %s);''', (student, datetime.utcnow()))


def get_student(student_id):
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor(cursor_factory=DictCursor)
        cur.execute('''SELECT * FROM students;''', (student_id,))
        print(cur.fetchone())


def add_course(course):
    with pg.connect(database='test_db', user='test', password=1234,
                    host='localhost', port=5432) as conn:
        cur = conn.cursor()
        cur.execute('''INSERT INTO course(name) VALUES (%s);''', (course,))


# create_db()
# add_course('Python')
# add_course('SQL')
# add_course('Machine learning')
# add_student('William')
# add_student('Stephen')
# get_student(2)
# add_student('Henry')
# add_student('Mary')
# add_student('Ann')
# add_students(1, 1)
# get_students(1)