import sys
import psycopg2
from datetime import date, timedelta


def main():
    db_host = sys.argv[1]
    db_user = sys.argv[2]
    db_password = sys.argv[3]
    db_db = sys.argv[4]
    res = execute_test_case(db_host, db_user, db_password, db_db)
    print("Execution Complete")

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def execute_test_case(db_host, db_user, db_password, db_db):
    db = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_db, port=5439)
    cursor = db.cursor()
    start_date = date(2016, 6, 4)
    end_date = date(2016, 6, 18)
    try:
        for single_date in daterange(start_date, end_date):
            sql_query="drop table if exists base.measurement_dictionary_"+single_date.strftime("%Y%m%d")
            print sql_query
            cursor.execute(sql_query)
        db.commit()
    except Exception as err:
        db.rollback()
        print err
    finally:
        db.close()

if __name__ == '__main__':
    main()
