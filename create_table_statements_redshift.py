import sys
import psycopg2

def main():
    db_host = sys.argv[1]
    db_user = sys.argv[2]
    db_password = sys.argv[3]
    db_db = sys.argv[4]
    fname = "input.txt"
    print("Execution Starts")
    execute_test_case(db_host, db_user, db_password, db_db, fname)
    print("Execution Complete")

def not_null_check(condition):
    if condition:
        return "not null"
    else:
        return ""

def add_encode(encode_type):
    if encode_type=="none":
        return ""
    else:
        return "encode "+str(encode_type)

def execute_test_case(db_host, db_user, db_password, db_db, fname):
    db = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_db, port=5439)
    cursor = db.cursor()
    write_sql_statement=""
    print("transaction starts")
    try:
        with open(fname) as f:
            contents = f.readlines()
            for content in contents:
                sql_query="set search_path to '$user', public,"+str(content).split('.')[0]+";select \"column\", type, encoding, \"notnull\" from pg_table_def where tablename = '"+str(content).split('.')[1].replace("\n","")+"' and schemaname='"+str(content).split('.')[0]+"';"
                cursor.execute(sql_query)
                results = cursor.fetchall()
                write_sql_statement+="drop table if exists "+str(content).replace("\n","")+";\n"
                write_sql_statement+="create table "+str(content).replace("\n","")+"(\n"
                count=len(results)
                for row in results:
                    write_sql_statement+=row[0]+" "+row[1]+" "+add_encode(row[2])+" "+not_null_check(row[3])
                    count-=1
                    if count != 0:
                        write_sql_statement+=",\n"
                write_sql_statement+=")"

                sql_query="set search_path to '$user', public,"+str(content).split('.')[0]+";select \"column\" from pg_table_def where tablename = '"+str(content).split('.')[1].replace("\n","")+"' and schemaname='"+str(content).split('.')[0]+"' and distkey = true;"
                cursor.execute(sql_query)
                results = cursor.fetchall()
                if len(results) != 0:
                    write_sql_statement+="\ndistkey("
                    for row in results:
                        for element in row:
                            write_sql_statement+=element+","
                        write_sql_statement=write_sql_statement[:-1]
                        write_sql_statement+=")"

                sql_query="set search_path to '$user', public,"+str(content).split('.')[0]+";select \"column\" from pg_table_def where tablename = '"+str(content).split('.')[1].replace("\n","")+"' and schemaname='"+str(content).split('.')[0]+"' and sortkey <> 0 order by sortkey;"
                cursor.execute(sql_query)
                results = cursor.fetchall()
                if len(results)!=0:
                    write_sql_statement+="\nsortkey("
                    for row in results:
                        for element in row:
                            write_sql_statement+=element+","
                    write_sql_statement=write_sql_statement[:-1]
                    write_sql_statement+=")"
                write_sql_statement+=";\n\n"
        text_file = open("final_output.sql", "w")
        text_file.write(write_sql_statement)
        text_file.close()
    except Exception as err:
        db.rollback()
        print err
    finally:
        db.close()

if __name__ == '__main__':
    main()
