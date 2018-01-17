import os
import csv
import json
import sqlite3 as sql
from collections import defaultdict


class csv_to_db(object):
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
    def __init__(self, filepath, delete_):
        print("CSV to MySQL database conversion initiated")
        if delete_ == True:
            try:
                os.remove('actor_plan.db')
            except Exception as noDB:
                print("DB doesn't exist")
            self.csv_db = sql.connect('actor_plan.db')
            self.csv_cursor = self.csv_db.cursor()
            # TODO: Allow variable definition of trips table, and table name
            self.csv_cursor.execute('''CREATE TABLE trips \
                (unique_id PRIMARYKEY VARCHAR(25),\
                pid KEY VARCHAR(20),\
                orig_maz MEDIUMINT,\
                dest_maz MEDIUMINT,\
                orig_purp CHAR(2),\
                dest_purp CHAR(2),\
                mode SMALLINT UNSIGNED,\
                depart_min FLOAT,\
                trip_dist FLOAT,\
                arrival_min FLOAT,\
                time_at_dest FLOAT);''')
        self.filepath = filepath

    def file_parsing(self):
        f_ = open(self.filepath, 'r')
        csv_reader = csv.reader(f_)
        csv_reader.__next__()
        actor_dict = defaultdict(list)
        for actor in csv_reader:
            if float(actor[32]) < 0:
                actor[32] = 0

            # Per-trip Data Struture (Derived from Disaggregate trip table data dictionary.docx) #

            # 0: Per household unique trip ID, 2: Household ID, 3: person number in HH
            # 19: Origin MAZ, 21: Dest. MAZ, 22: origin purpose, 23: dest. purpose, 24: Mode of travel
            # 26: Depart minute, 27: Trip dist., 29: Travel minute, 31: Arrival min, 32: Time at dest.

            # Using simplifed purpose dictionary based on config file
            # TODO: Provide file input for purpose dictionaries
            uuid = ('{}_{}_{}').format(str(actor[0]), str(actor[2]), str(actor[3]))
            pid = ('{}_{}').format(str(actor[2]), str(actor[3]))
            orig_purp = self.purpose_dict[str(actor[22])]
            dest_purp = self.purpose_dict[str(actor[23])]

            # TODO: Allow variable input of actor data params
            actor_data = [uuid,\
                            pid,\
                            int(actor[19]),\
                            int(actor[21]),\
                            orig_purp,\
                            dest_purp,\
                            int(actor[24]),\
                            float(actor[26]),\
                            float(actor[27]),\
                            float(actor[29]),\
                            float(actor[31])
                        ]
            self.csv_cursor.execute('''INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)''', tuple(actor_data))
            actor_dict[pid].append(actor_data)
            # if pid in actor_dict:
            #     actor_dict[pid].append(actor_data)
            # else:
            #     actor_dict[pid] = [actor_data]

        self.csv_db.commit()
        for actor in actor_dict:
            actor_dict[actor] = sorted(actor_dict[actor], key=lambda x: x[7])
        with open("plans_by_maz_and_pid.json", 'w') as handle:
            json.dump(actor_data, handle)

    def __del__(self):
        self.csv_db.close()

if (__name__ == "__main__"):
    testing = csv_to_db("output_disaggTripList.csv", True)
    testing.file_parsing()
    print(testing.csv_cursor.execute("SELECT * FROM trips limit 10;").fetchall())

