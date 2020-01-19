import psycopg2


class DBHelper:
    def __init__(self):
        # Establish connection with postgreSQL
        self.__connection = psycopg2.connect(
            host="localhost",
            database="jumo",
            user="postgres",
            password="postgres",
            port=5432
        )

    def write_records(self, rows, table, schema="jumo_now"):
        # Create cursor
        cursor = self.__connection.cursor()
        try:
            for row in rows:
                sql = f"insert into {schema}.{table} values ("
                for key in row:
                    sql = sql + f"%({key})s, "
                sql = sql[:-2] + ")"
                cursor.execute(sql, row)
                self.__connection.commit()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            self.__connection.close()
