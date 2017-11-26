import sqlite3 as sql
import os
import csv

class csv_to_db(object):

    def __init__(self, filepath, delete_):
        print("CSV to MySQL database conversion initiated")
        if delete_ == True:
            try:
                os.remove('csv.db')
            except Exception as noDB:
                print("DB doesn't exist")
            self.csv_db = sql.connect('csv.db')
            self.csv_cursor = self.csv_db.cursor()
            self.csv_cursor.execute('''CREATE TABLE trips (pnum MEDIUMINT KEY, orig_maz MEDIUMINT, dest_maz MEDIUMINT, mode SMALLINT UNSIGNED, depart_min FLOAT, time_at_dest FLOAT);''')
        else:
            self.csv_db = sql.connect('csv.db')
            self.csv_cursor = self.csv_db.cursor()

        self.filepath = filepath

    def file_parsing(self):
        f_ = open(self.filepath, 'r')
        csv_reader = csv.reader(f_)
        csv_reader.__next__()
        for actor in csv_reader:
            try:
                if float(actor[32]) < 0:
                    actor[32] = 0
                #pnum, origMaz, destMaz, mode, finalDepartMin, timeAtDest
                exec_str = ('INSERT INTO trips VALUES ({0}, {1}, {2}, {3}, {4}, {5});').format(int(actor[3]), int(actor[19]), int(actor[21]), int(actor[24]), float(actor[27]), float(actor[32]))
                self.csv_cursor.execute(exec_str)
            except ValueError as valErr:
                print(actor[3],actor[19],actor[21],actor[24],actor[27],actor[32])
                continue
        self.csv_db.commit()

    def __del__(self):
        self.csv_db.close()

if (__name__ == "__main__"):
    testing = csv_to_db("output_disaggTripList.csv", True)
    testing.file_parsing()
    print(testing.csv_cursor.execute("SELECT * FROM trips limit 10;").fetchall())

