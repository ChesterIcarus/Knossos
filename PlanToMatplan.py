import os
import csv
import json
import math
import pickle
import sqlite3 as sql
import pymysql as mysql
import getpass
import numpy as np
# from __init__ import bounding_for_maz
from collections import defaultdict
from shapely.geometry import shape, polygon

class PlanToMatplan(object):
    mode_dict = {
       "1": "car",
       "2": "car",
       "3": "car",
       "4": "car",
       "5": "walk",
       "6": "car",
       "7": "car",
       "8": "walk",
       "9": "car",
       "10": "car",
       "11": "walk",
       "12": "bike",
       "13": "walk",
       "14": "car"
    }
    def __init__(self):
        print("Converting agent plans to MATplans")
        print("This module provides the functionality to translate from agent plans with movement defined by MAZ's\n")
        print("Into agents plans with defined coordinates/APN's")
        self.maz_set = None
        self.actor_dict = defaultdict(list)
        self.plan_rows = dict()
        self.plan_conn = None
        self.plan_cur = None
        self.plan_table_name = None
        self.apn_conn = None
        self.apn_cur = None
        self.apn_table_name = None

    def connect_plan_db(self, plan_table_name, database=None, _file=None, drop=False):
        if ((_file != None) and (database == None)):
            self.plan_conn = sql.connect(_file)
            self.plan_cur = self.plan_conn.cursor()
        elif (database != None and _file == None):
            self.plan_conn = mysql.connect(user=database['user'],
                        password=database['password'],
                        db=database['db'],
                        host=database['host'])
            self.plan_cur = self.plan_conn.cursor()
            self.plan_table_name = plan_table_name
            if drop:
                exec_str = f"DROP TABLE IF EXISTS {self.plan_table_name}"
                self.plan_cur.execute(exec_str)
                exec_str = f"""CREATE TABLE {self.plan_table_name}
                (pid VARCHAR(25),
                origAPN CHAR(12),
                destAPN CHAR(12),
                origCoord_x FLOAT,
                origCoord_y FLOAT,
                destCoord_x FLOAT,
                destCoord_y FLOAT,
                mode CHAR(4),
                origPurp CHAR(2),
                destPurp CHAR(2),
                finalDepartTimeSec FLOAT,
                arrivalTimeSec FLOAT,
                timeAtDestSec FLOAT)"""
                self.plan_cur.execute(exec_str)

    def connect_apn_db(self, apn_table_name, database=None, _file=None):
        if ((_file != None) and (database == None)):
            self.apn_conn = sql.connect(_file)
            self.apn_cur = self.apn_conn.cursor()

        elif (database != None and _file == None):
            self.apn_conn = mysql.connect(user=database['user'],
                        password=database['password'],
                        db=database['db'],
                        host=database['host'])
            self.apn_cur = self.apn_conn.cursor()
            self.apn_table_name = apn_table_name

    def bounded_maz_creation(self, preprocessed_files, output_file, maz_file=None, maz_in_memory=None, overwrite=False, linked_dict=None, bounding=None):
        self.valid_maz_list = list()
        self.maz_dict = dict()
        if (preprocessed_files == None):
            if (maz_in_memory == None) and (maz_file != None):
                with open(maz_file, 'r') as maz_f_:
                    maz_set = json.load(maz_f_)
            elif ((maz_in_memory != None) and (maz_file == None)):
                maz_set = self.maz_set
            else:
                raise ValueError(None)

            all_in_bounding = True
            if bounding is None:
                bounding_for_maz = polygon.LinearRing([
                    (649054, 896498),
                    (649192, 888749),
                    (665535, 896129),
                    (663998, 889026)])
            else:
                bounding_for_maz = polygon.LinearRing(bounding)

            for index, maz in enumerate(maz_set['features']):
                try:
                    add_to_set = True
                    if (all_in_bounding == False):
                        if not ((shape(maz['geometry'])).representative_point()).within(bounding_for_maz):
                            add_to_set = False
                    if (add_to_set):
                        rep = (shape(maz['geometry'])).representative_point()
                        try:
                            maz_set['features'][index]['geometry']['coordinates'] = rep.coords[0]
                            maz_set['features'][index]['geometry']['type'] = "Point"
                        except IndexError as idxErr:
                            maz_set['features'][index]['geometry']['coordinates'] = rep.coords
                            maz_set['features'][index]['geometry']['type'] = "Point"
                        self.maz_dict[maz['properties']['MAZ_ID_10']] = maz_set['features'][index]
                        self.valid_maz_list.append(maz['properties']["MAZ_ID_10"])
                    else:
                        maz_set['features'].remove(maz)
                except ValueError as topErr:
                    if (shape(maz['geometry'])).within(bounding_for_maz):
                        self.maz_dict[maz['properties']['MAZ_ID_10']] = maz_set['features'][index]
                        self.valid_maz_list.append(maz['properties']["MAZ_ID_10"])
                    else:
                        maz_set['features'].remove(maz)
            if (os.path.isfile(output_file) == False) or (overwrite == True):
                with open("dict_" + output_file, 'w+') as handle:
                    json.dump(list(self.maz_dict.values()), handle)
                with open("list_" + output_file, 'w+') as handle:
                    for val in self.valid_maz_list:
                        handle.write(f"{val}\n")
        else:
            if (linked_dict == None):
                self.maz_dict = json.load(open(preprocessed_files['dict'],'r'))
            else:
                self.maz_dict = linked_dict
            with open(preprocessed_files['list'], 'r') as handle:
                l_ = handle.read().splitlines()
                for _ in l_:
                    self.valid_maz_list.append(int(_))

    def load_plans_from_json(self, filename):
        self.plan_rows = json.load(open(filename,'r'))
        print("Loaded")

    def load_plans_from_sqlite(self, sqlite_db_name, pid_maz_table_name):
        con = sql.connect(sqlite_db_name)
        cur = con.cursor()
        rows_query = (f"SELECT * from {pid_maz_table_name}")
        self.plan_rows = cur.execute(rows_query).fetchall()

    def load_plans_from_db(self, cursor, pid_maz_table_name):
        rows_query = (f"SELECT * from {pid_maz_table_name}")
        cursor.execute(rows_query)
        self.plan_rows = cursor.fetchall()
        # exec_str = (f"SELECT DISTINCT maz from {pid_maz_table_name}")

    def maz_to_plan_coords(self, apn_table_name, apn_selector):
        ''' 
            Plan DB schema for file actor_plan.db, on table "trips"
                0: unique_id (varchar(25)) (PRIMARYKEY)
                1: pid (varchar(20)) (KEY)
                2: orig_maz (int)
                3: dest_maz (int)
                4: orig_purp (char(2))
                5: dest_purp (char(2))
                6: mode (unsigned smallint)
                7: depart_min (float)
                8: trip_dist (float)
                9: arrival_min (float)
                10: time_at_dest (float)
            Coordinates to APN and MAZ DB schema for MySQL: LinkingAPNtoMAZ, on table "Example"
                1: coordX (float)
                2: coordY (float)
                3: APN (char(12))
                4: MAZ (int(10)) '''
        orig_apn = None
        dest_apn = None
        orig_x = None
        orig_y = None
        dest_x = None
        dest_y = None
        count = 0
        for row in self.plan_rows:
            # Testing if maz is in valid defined subset
            add_to_dict = False
            actor_id = row[1]
            orig_maz = int(row[2])
            dest_maz = int(row[3])
            prev_act = dict()

            while not add_to_dict and (count < 10):
                if (((row[2] in self.valid_maz_list) and (row[3] in self.valid_maz_list)) or (row[1] in self.actor_dict)):
                    if (row[1] in self.actor_dict) and (count == 0):
                        orig_apn = self.actor_dict[row[1]][len(self.actor_dict[row[1]])-1]['destAPN']
                        prior_maz = self.actor_dict[row[1]][len(self.actor_dict[row[1]])-1]['origMaz']
                    else:
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}").format(apn_table_name, apn_selector, row[2])
                        orig_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(orig_apn) <= 0: orig_apn = None; break
                        orig_apn = orig_apn[numpy.random.randint(-1, len(orig_apn)-1)][0]
                    if (row[3] not in self.valid_maz_list):
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}"\
                            ).format(apn_table_name, apn_selector, prior_maz)
                        dest_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(dest_apn) <= 0: dest_apn = None; break
                        dest_apn = dest_apn[numpy.random.randint(-1, len(dest_apn)-1)][0]
                    else:
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}"\
                            ).format(apn_table_name, apn_selector, row[3])
                        dest_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(dest_apn) <= 0: dest_apn = None; break
                        dest_apn = dest_apn[numpy.random.randint(-1, len(dest_apn)-1)][0]
                else:
                    break
                count += 1

            if (dest_apn != orig_apn) and (dest_apn != None) and (orig_apn != None):
                earliest_MAG = float(4.5 * 60)
                depart_time_in_secs = float(row[7]) * 60 + earliest_MAG
                arrive_time_in_secs = float(row[9]) * 60 + earliest_MAG
                at_dest_time_in_secs = float(row[10]) * 60
                act_dict = {'origAPN': orig_apn,
                                                'destAPN':dest_apn,
                                                'origCoord_x': orig_x,
                                                'origCoord_y': orig_y,
                                                'destCoord_x': dest_x,
                                                'destCoord_y': dest_y,
                                                'mode':self.mode_dict[str(row[6])],
                                                'origPurp': str(row[4]),
                                                'destPurp': str(row[5]),
                                                'arrivalTimeSec': arrive_time_in_secs,
                                                'finalDepartTimeSec': depart_time_in_secs,
                                                'timeAtDestSec': at_dest_time_in_secs}
                # prev_act = act_dict
                # prev_act['origMaz'] = row[2]
                self.actor_dict[row[1]].append(act_dict)
            count = 0
            orig_apn = None
            dest_apn = None

    def plan_to_sql(self):
        if self.plan_conn == None:
            raise ConnectionError
        insert_list = list()
        for itm in self.actor_dict:
            for index in range(0, len(self.actor_dict[itm])):
                y = self.actor_dict[itm][index]
                data_dict = {'pid': itm,
                            'origAPN': str(y['origAPN']),
                            'destAPN': str(y['destAPN']),
                            'origCoord_x': float(y['origCoord_x']),
                            'origCoord_y': float(y['origCoord_y']),
                            'destCoord_x': float(y['destCoord_x']),
                            'destCoord_y': float(y['destCoord_y']),
                            'mode': str(y['mode']),
                            'origPurp': str(y['origPurp']),
                            'destPurp': str(y['destPurp']),
                            'finalDepartTimeSec': y['finalDepartTimeSec'],
                            'arrivalTimeSec': y['arrivalTimeSec'],
                            'timeAtDestSec': y['timeAtDestSec']}
                insert_list.append(tuple(data_dict.values()))
                # exec_str = (f"INSERT INTO {self.plan_table_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                # self.plan_cur.execute(exec_str, tuple(data_dict.values()))
        exec_str = (f"INSERT INTO {self.plan_table_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        self.plan_cur.execute(exec_str, tuple(insert_list))
        self.plan_conn.commit()
        self.plan_conn.close()

    def plan_to_txt_file(self, output_file):
        with open(output_file, 'w') as handle:
            for itm in self.actor_dict:
                for index in range(0, len(self.actor_dict[itm])):
                    y = self.actor_dict[itm][index]
                    x = ("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}\n").format(
                            itm,
                            y['origAPN'],
                            y['destAPN'],
                            y['origCoord_x'],
                            y['origCoord_y'],
                            y['destCoord_x'],
                            y['destCoord_y'],
                            y['mode'],
                            y['origPurp'],
                            y['destPurp'],
                            y['finalDepartTimeSec'],
                            y['arrivalTimeSec'],
                            y['timeAtDestSec'])
                    handle.write(x)

if __name__ == "__main__":
    example = PlanToMatplan()
    pw = getpass.getpass()
    apn_db_param = {'user':'root', 'db':'linkingApnToMaz', 'host':'localhost', 'password': pw}
    plan_db_param = {'user':'root', 'db':'plansByAPN', 'host':'localhost', 'password': pw}
    pid_maz_db_param = {'user':'root', 'db':'magDataToPlansByPidAndMaz', 'host':'localhost', 'password': pw}

    example.connect_apn_db(database=apn_db_param, apn_table_name='Example')
    example.connect_plan_db(database=plan_db_param, plan_table_name='Example', drop=True)

    con = mysql.connect(user=pid_maz_db_param['user'],
                        password=pid_maz_db_param['password'],
                        db=pid_maz_db_param['db'],
                        host=pid_maz_db_param['host'])
    cur = con.cursor()

    example.load_plans_from_db(cursor=cur, pid_maz_table_name="Example")
    ppf_dict = {'dict': './dict_PlanToMatplan.txt', 'list': './list_PlanToMatplan.txt'}
    full_ariz = [
        (291681.866638, 2147002.203025),
        (1114836.32474, 2099088.372318),
        (1092055.717785, 913579.224235),
        (341912.702254, 946252.321431)]
    example.bounded_maz_creation(preprocessed_files=None, maz_file='../Data/maz.geojson', output_file="PlanToMatplan.txt", overwrite=True, bounding=None)
    # example.load_plans_from_json( JSON FILE HERE)
    # example.load_plans_from_sqlite("actor_plan.db", "trips")
    example.maz_to_plan_coords(example.apn_table_name, "maz")
    example.plan_to_txt_file("Plans_from_PlanToMatplan.txt")
    example.plan_to_sql()
