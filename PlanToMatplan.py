import os
import csv
import json
import pickle
import sqlite3 as sql
import numpy
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
        print("Converting user plans to MATplans")
        self.maz_set = None
        self.actor_dict = defaultdict(list)
        self.plan_rows = dict()

    def connect_plan_db(self, database=None, _file=None):
        if ((_file != None) and (database == None)):
            self.plan_conn = sql.connect(_file)
            self.plan_cur = self.plan_conn.cursor()

    def connect_apn_db(self, database=None, _file=None):
        if ((_file != None) and (database == None)):
            self.apn_conn = sql.connect(_file)
            self.apn_cur = self.apn_conn.cursor()

    def bounded_maz_creation(self, preprocessed_file, output_file, maz_file=None, maz_in_memory=None, overwrite=False, linked_dict=None):
        self.valid_maz_list = list()
        self.maz_dict = dict()
        if (preprocessed_file == None):
            if (maz_in_memory == None) and (maz_file != None):
                with open(maz_file, 'r') as maz_f_:
                    maz_set = json.load(maz_f_)
            elif ((maz_in_memory != None) and (maz_file == None)):
                maz_set = self.maz_set
            else:
                raise ValueError(None)
            
            all_in_bounding = True
            bounding_for_maz = polygon.LinearRing([(649054, 896498), (649192, 888749), (665535, 896129), (663998, 889026)])

            for index, maz in enumerate(maz_set['features']):
                try:
                    add_to_set = True
                    if (all_in_bounding == False):
                        if not ((shape(maz['geometry'])).representative_point()).within(bounding_for_maz):
                            add_to_set = False
                    if (add_to_set == True):
                        rep = (shape(maz['geometry'])).representative_point()
                        try:
                            maz_set['features'][index]['geometry']['coordinates'] = rep.coords[0]
                            maz_set['features'][index]['geometry']['type'] = "Point"
                        except IndexError as idxErr:
                            maz_set['features'][index]['geometry']['coordinates'] = rep.coords
                            maz_set['features'][index]['geometry']['type'] = "Point"
                        self.maz_dict[maz['properties']['MAZ_ID_10']] = maz_set['features'][index]
                    else:
                        maz_set['features'].remove(maz)
                except ValueError as topErr:
                    if (shape(maz['geometry'])).within(bounding_for_maz):
                        self.maz_dict[maz['properties']['MAZ_ID_10']] = maz_set['features'][index]
                        self.valid_maz_list.append(maz['properties']["MAZ_ID_10"])
                    else:
                        maz_set['features'].remove(maz)
            if (os.path.isfile(output_file) == False) or (overwrite == True):
                with open(output_file, 'w') as handle:
                    json.dump(list(self.maz_dict.values()), handle)
        else:
            if (linked_dict == None):
                self.maz_dict = json.load(open(preprocessed_file,'r'))
            else:
                self.maz_dict = linked_dict
            self.valid_maz_list = list(self.maz_dict.keys())

    def load_plans_from_json(self, filename):
        self.plan_rows = json.load(open(filename,'r'))
        print("Loaded")

    def load_plans_from_sqlite(self, table):
        rows_query = ("SELECT * from {}").format(table)
        self.plan_rows = self.plan_cur.execute(rows_query).fetchall()

    def maz_to_plan_db(self, apn_table, apn_selector, output_file):
        orig_apn = None
        dest_apn = None
        count = 0
        for row in self.plan_rows:
            # Testing if maz is in valid defined subset
            while ((orig_apn == dest_apn) and (count < 10)):
                if (((row[2] in self.valid_maz_list) and (row[3] in self.valid_maz_list)) or (row[1] in self.actor_dict)):
                    if (row[1] in self.actor_dict) and (count == 0):
                        orig_apn = self.actor_dict[row[1]][len(self.actor_dict[row[1]])-1]['destAPN']
                        prior_maz = self.actor_dict[row[1]][len(self.actor_dict[row[1]])-1]['origMaz']
                    else:
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}").format(apn_table, apn_selector, row[2])
                        orig_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(orig_apn) <= 0: orig_apn = None; break
                        orig_apn = orig_apn[numpy.random.randint(-1, len(orig_apn)-1)][0]
                    if (row[3] not in self.valid_maz_list):
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}"\
                            ).format(apn_table, apn_selector, prior_maz)
                        dest_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(dest_apn) <= 0: dest_apn = None; break
                        dest_apn = dest_apn[numpy.random.randint(-1, len(dest_apn)-1)][0]
                    else:
                        exec_str = ("SELECT * FROM {0} WHERE {1} = {2}"\
                            ).format(apn_table, apn_selector, row[3])
                        dest_apn = self.apn_cur.execute(exec_str).fetchall()
                        if len(dest_apn) <= 0: dest_apn = None; break
                        dest_apn = dest_apn[numpy.random.randint(-1, len(dest_apn)-1)][0]
                else:
                    break
                count += 1
            if (dest_apn != orig_apn) and (dest_apn != None) and (orig_apn != None):
                depart_time = (float(row[7]) * (30 * 60) + (4.5 * 60 * 60)) / 60
                travel_time = (float(row[9]) * (30 * 60) + (4.5 * 60 * 60)) / 60
                arrive_time = (float(row[10]) * (30 * 60) + (4.5 * 60 * 60)) / 60
                self.actor_dict[row[1]].append({'origAPN': orig_apn, 'destAPN':dest_apn, \
                    'mode':self.mode_dict[str(row[6])], 'origPurp': str(row[4]), 'destPurp': str(row[5]), \
                    'finalDepartMin':str(depart_time), 'timeAtDest': None,\
                    'origMaz': int(row[2]) if (row[2] in self.valid_maz_list) else (prior_maz),\
                    'travelMin':travel_time , 'tripDistance': float(row[8])})
            count = 0
            orig_apn = None
            dest_apn = None

    # with (open('actor_plans.json', 'w')) as handle:
        #     json.dump(self.actor_dict, handle)
        with open(output_file, 'w') as handle:
            for itm in self.actor_dict:
                for index in range(0, len(self.actor_dict[itm])):
                    y = self.actor_dict[itm][index]
                    x = ("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}\n").format(\
                            itm,\
                            y['origAPN'],\
                            y['destAPN'],\
                            y['origCoord_x'],\
                            y['origCoord_y'],\
                            y['destCoord_x'],\
                            y['destCoord_y'],\
                            y['mode'],\
                            y['origPurp'],\
                            y['destPurp'],\
                            y['timeAtDest'],\
                            y['finalDepartMin'],\
                            y['travelMin'])
                    handle.write(x)

