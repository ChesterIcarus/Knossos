import os
import csv
import json
import getpass
import sqlite3 as sql
# import pymysql as mysql
import MySQLdb as mysql
from collections import defaultdict


class MagDataToPlansByPidAndMaz:
    purpose_dict = {
        "0": "h",
        "1": "w",
        "2": "w",
        "3": "w",
        "411": "w",
        "412": "w",
        "42": "w",
        "5": "s",
        "6": "w",
        "4": "w",
        "7": "s",
        "71": "s",
        "72": "s",
        "73": "s",
        "8": "s",
        "9": "s",
        "10": "s",
        "15": "w",
    }

    def __init__(self):
        print("MAG Data to Plans by PID and MAZ initialized")
        self.actor_dict = defaultdict(list)
        self.cur = None
        self.conn = None
        self.table_name = None

    def connect_database(self, database, table_name, drop):
        '''Connecting to Database used to store the output, if the output is to be written to a database.
        Input is of the form: database = 
            {'host': port, 'user': Valid Database User,
            'password': Password for the specified user, 'db': Database to use},
            table_name =  Name of the table to use (Must exist if drop is False),
            drop =  Boolean indicating if we are to drop existing table='tablename
        Output is stored in the form: (X Coordinate, Y Coordinate, APN Identifier, MAZ Identifier)'''

        print("CSV to MySQL database conversion initiated")
        self.conn = mysql.connect(**database)
        self.cur = self.conn.cursor()
        self.table_name = table_name
        if drop == True:
            self.cur.execute(("DROP TABLE if exists {}").format(table_name))
        # exec_str = ('CREATE TABLE {} \n'
        #             '            (unique_id PRIMARY KEY VARCHAR(25),\n'
        #             '            pid KEY VARCHAR(20),\n'
        #             '            orig_maz MEDIUMINT,\n'
        #             '            dest_maz MEDIUMINT,\n'
        #             '            orig_purp CHAR(2),\n'
        #             '            dest_purp CHAR(2),\n'
        #             '            mode SMALLINT UNSIGNED,\n'
        #             '            depart_min FLOAT,\n'
        #             '            trip_dist FLOAT,\n'
        #             '            arrival_min FLOAT,\n'
        #             '            time_at_dest FLOAT)').format(table_name)
        self.cur.execute(('CREATE TABLE {0} \n'
                    '            (unique_id VARCHAR(25),\n'
                    '            pid VARCHAR(20),\n'
                    '            orig_maz MEDIUMINT,\n'
                    '            dest_maz MEDIUMINT,\n'
                    '            orig_purp CHAR(2),\n'
                    '            dest_purp CHAR(2),\n'
                    '            mode SMALLINT UNSIGNED,\n'
                    '            depart_min FLOAT,\n'
                    '            trip_dist FLOAT,\n'
                    '            arrival_min FLOAT,\n'
                    '            time_at_dest FLOAT)').format(table_name))

    def read_mag_csv(self, filepath):
        '''Takes MAG data as a CSV, and parses the file to a dictionary.
        This dict is stored at a class level defaultly, and can be written
        to an output file as well.'''
        f_ = open(filepath, 'r')
        csv_reader = csv.reader(f_)
        csv_reader.__next__()
        # Per-trip Data Struture (Derived from Disaggregate trip table data dictionary.docx)
        # 0: Per household unique trip ID, 2: Household ID, 3: person number in HH
        # 19: Origin MAZ, 21: Dest. MAZ, 22: origin purpose, 23: dest. purpose, 24: Mode of travel
        # 26: Depart minute, 27: Trip dist., 29: Travel minute, 31: Arrival min, 32: Time at dest.
        # Using simplifed purpose dictionary based on config file
        for actor in csv_reader:
            # Setting negative time-at-destinations to 0
            if float(actor[32]) < 0:
                actor[32] = 0

            uuid = ('{}_{}_{}').format(str(actor[0]), str(actor[2]), str(actor[3]))
            pid = ('{}_{}').format(str(actor[2]), str(actor[3]))

            orig_purp = self.purpose_dict[str(actor[22])]
            dest_purp = self.purpose_dict[str(actor[23])]

            actor_data = [uuid, pid, int(actor[19]), int(actor[21]),
                          orig_purp, dest_purp, int(actor[24]),
                          float(actor[31]), float(actor[27]), 
                          float(actor[29]), float(actor[26])]
            self.actor_dict[pid].append(actor_data)
        for actor in self.actor_dict:
            self.actor_dict[actor] = sorted(self.actor_dict[actor], key=lambda x: x[7])

    def write_mag_to_sql(self):
        for actor in self.actor_dict:
            for x in self.actor_dict[actor]:
                exec_str = f"INSERT INTO {self.table_name} VALUES {tuple(x)}"
            self.cur.execute(exec_str)
        self.conn.commit()

    def write_mag_to_file(self, filepath):
        with open(filepath, 'w+') as handle:
            json.dump(self.actor_dict, handle)

    # def __del__(self):
    #     self.conn.close()

if (__name__ == "__main__"):
    database = {}
    pw = getpass.getpass()
    db_param = {'user':'root', 'db':'MagDataToPlansByPidAndMaz', 'host':'localhost', 'password': pw}
    example = MagDataToPlansByPidAndMaz()
    example.connect_database(db_param, table_name='Example', drop=True)
    example.read_mag_csv("output_disaggTripList.csv")
    example.write_mag_to_sql()
    # example.write_mag_to_file("MagDataToPlan_output_Example.txt")


