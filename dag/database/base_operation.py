import pymysql.cursors
import pandas as pd
import time
from pymysqlpool.pool import Pool


class DBConn:
    """
    Class for handling the database connection
    """

    def __init__(self):
        self.host = "localhost"
        self.port = 3306
        self.user = "root"
        self.password = "root"
        self.db = "employees"
        self.conn = self.__create_pool_connection()

    def __create_pool_connection(self):
        """
        Create pooled connection to the target database.
        """
        pool = Pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            cursorclass=pymysql.cursors.DictCursor,
        )
        pool.init()
        return pool


class InternalServerError(Exception):
    pass


class DBOperations:
    """
    Base class for all of the DB operations occurred
    """

    def __init__(self, conn: DBConn):
        self.conn = DBConn().conn

    def execute_many(self, query: str, data: list) -> dict:
        """
        Execute query MANY TIMES from connection pool and return dict results.
        Use this for INSERT of UPDATE queries. Need list as data input.
        """
        try:

            _conn = self.conn.get_conn()
            with _conn.cursor() as cur:
                cur.executemany(query, data)

            _conn.commit()
            res = {"status": True}

            # release from connection pool
            self.conn.release(_conn)
            return res
        except Exception as e:
            raise InternalServerError(e)

    def insert_employees_record(self, data: pd.DataFrame) -> bool:
        """
        Insert data based on list of inputted data
        and update based on key if duplicate key found
        """
        start = time.time()
        temp_data = list()
        counter = 0
        for row in data.itertuples(index=False, name=None):
            counter += 1
            temp_data.append((row[0], row[1], row[2], row[3], row[4]))
            if counter % 1000 == 0:
                self.execute_many(
                    f"""
                        INSERT INTO
                            `employee` (
                                            employee_id,
                                            branch_id,
                                            salary,
                                            join_date,
                                            resign_date
                                        )
                        VALUES
                            (%s, %s, %s, %s, %s)
                        ON
                            DUPLICATE KEY UPDATE 
                            branch_id= VALUES(branch_id), 
                            salary=VALUES(salary),
                            resign_date=VALUES(resign_date);

                    """,
                    temp_data,
                )
                temp_data = list()
        end = time.time()
        print("Insert to DB:", round((end - start), 5), "sec")

    def insert_timesheet_record(self, data: pd.DataFrame) -> bool:
        """
        Insert data based on list of inputted data
        and update based on key if duplicate key found
        """
        start = time.time()
        data = data.where(
            pd.notnull(data), None
        )  # replace nan with None before inserting
        temp_data = list()
        counter = 0
        for row in data.itertuples(index=False, name=None):
            counter += 1
            temp_data.append((row[0], row[1], row[2], row[3], row[4]))
            if counter % 1000 == 0:
                self.execute_many(
                    f"""
                    INSERT INTO
                        `timesheet` (
                                        timesheet_id,
                                        employee_id,
                                        date,
                                        checkin,
                                        checkout
                                    )
                    VALUES
                        (%s, %s, %s, %s, %s)
                    ON
                        DUPLICATE KEY UPDATE 
                        checkin= VALUES(checkin), 
                        checkout=VALUES(checkout);

                    """,
                    temp_data,
                )
                temp_data = list()
        end = time.time()
        print("Insert to DB:", round((end - start), 5), "sec")
