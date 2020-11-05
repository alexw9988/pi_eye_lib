
import logging
import uuid
from collections.abc import Iterable, MutableMapping

import psycopg2

log = logging.getLogger('DBClient')


SQL_create_camera_table = \
    """
    CREATE TABLE IF NOT EXISTS camera_table (
        id                      serial PRIMARY KEY,
        count                   integer,
        barrier_falling_time    timestamp,
        barrier_rising_time     timestamp,
        cam_start_time          timestamp,
        cam_stop_time            timestamp,
        cam_settings            text,
        cam_status              smallint
    );
    """


SQL_create_scanner_table = \
    """
    CREATE TABLE IF NOT EXISTS scanner_table (
        id                      serial PRIMARY KEY,
        leicht_id               text,
        scan_time               timestamp
    );
    """


SQL_create_processor_table = \
    """
    CREATE TABLE IF NOT EXISTS processor_table (
        id                      serial PRIMARY KEY,
        scan_id                 integer,
        camera_id               integer,
        proc_start_time         timestamp,
        proc_end_time           timestamp,
        proc_settings           text,
        proc_result             double precision,
        proc_status             smallint,
        faulty                  boolean
    );
    """


valid_tables = ['camera_table', 'scanner_table', 'processor_table']


class DatabaseClient():

    def __init__(self):
        pass

    def close(self):
        pass

    def insertRow(self, table, vals, return_id=False):
        if return_id:
            return int(uuid.uuid4())

    def insertRows(self, table, vals_list, return_id=False):
        retval = [self.insertRow(table, vals, return_id=return_id) for vals in vals_list]
        if return_id:
            return retval

    
class PostgreSQLClient(DatabaseClient):

    def __init__(self, dbname='sequencedb', user='postgres', password='postgres123', host='localhost', port=5432):
        """ Create and return a PostgreSQL database client """
        self.dbname = dbname
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self._createTables()

    def close(self):
        """ Close the connection. Call this function before destruction of the client. """
        self.conn.close()

    def insertRow(self, table, vals, return_id=False):
        """
        Insert a row into the table.

        Parameters
        ----------
        table : str
            Name of the table in the database.
            Currently supported: 'camera', 'scanner', 'processor'
        vals : dict
            Dictionary keys are column the names, values are the insert values. 
            Pass timestamps as datetime.datetime objects, otherwise only int, float, string are
            supported! 
        return_id : bool
            If true, the function will return the integer ID of the inserted row.
        """

        query = self._createInsertQuery(table, vals, return_id=return_id)
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
            if return_id:
                return cur.fetchone()[0]

    def insertRows(self, table, vals_list, return_id=False):
        """
        Insert multiple rows into the table.

        Parameters
        ----------
        table : str
            Name of the table in the database.
            Currently supported: 'camera', 'scanner', 'processor'
        vals : list of dicts
            List of row entries, passed as dictionaries.
            Dictionary keys are column the names, values are the insert values. 
            Pass timestamps as datetime.datetime objects, otherwise only int, float, string are
            supported! 
        return_id : bool
            If true, the function will return a list of integer ID of the inserted rows.
        """

        retval = [self.insertRow(table, vals, return_id=return_id) for vals in vals_list]
        if return_id:
            return retval

    def _createInsertQuery(self, table, vals, return_id=False):
        if not isinstance(vals, MutableMapping):
            raise Exception("SQL columns and values must be passed as dict-like!")
        
        if not table.endswith("_table"):
            table = table + "_table"
        if not table in valid_tables:
            raise Exception("Invalid table: {}".format(table))

        columns = ', '.join(str(x) for x in vals.keys())
        values = ', '.join("'" + str(x).replace("'", "''") + "'" for x in vals.values())
        if return_id:
            query = f"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING id;"
        else:
            query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
        
        return query

    def _createTables(self):
        for table in valid_tables:
            with self.conn.cursor() as cur:
                if table == 'camera_table':
                    cur.execute(SQL_create_camera_table)
                elif table == 'scanner_table':
                    cur.execute(SQL_create_scanner_table)
                elif table == 'processor_table':
                    cur.execute(SQL_create_processor_table)
                self.conn.commit()
