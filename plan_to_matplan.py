import os
import csv
import json
import pickle
import sqlite3 as sql
# from __init__ import bounding_for_maz
from shapely.geometry import shape, polygon

class plan_to_matplan(object):

    def __init__(self):
        print("Converting user plans to MATplans")
    
    def from_db(self, database, filepath):
        self.plan_conn = sql.connect(database['plan_database'])
        self.plan_cur = self.plan_conn.cursor()

        self.apn_conn = sql.connect(database['apn_database'])
        self.apn_cur = self.apn_conn.cursor()
        self.actor_dict = dict()
        # Placeholder for now
        self.orig_apn = {"1":"1"}
        self.dest_apn = {"1":"1"}

        # Copied from APN for the time being
        maz_file = open(filepath['maz'], 'r')
        maz_set = json.load(maz_file)
        maz_file.close()
        maz_dict = dict()
        bounding_for_maz = polygon.Polygon([(564413, 892531), (618474, 892531), (564413, 894696), (618474, 894696)])
        if (os.path.isfile('maz_dict.pickle') == False):
            for maz in maz_set['features']:
                try:
                    if ((shape(maz['geometry'])).representative_point()).within(bounding_for_maz):
                        maz_dict[maz['properties']['MAZ_ID_10']] = "True"
                    else:
                        maz_set['features'].remove(maz)
                except ValueError as topErr:
                    if (shape(maz['geometry'])).within(bounding_for_maz):
                        maz_dict[maz['properties']['MAZ_ID_10']] = "True"
                    else:
                        maz_set['features'].remove(maz)
            with open('maz_dict.pickle', 'wb') as handle:
                pickle.dump(maz_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            maz_dict = pickle.load(open('maz_dict.pickle','rb'))
        if (os.path.isfile('plan_list.pickle') == False):
            self.plan_cur.execute(("SELECT * from {0}").format(database['plan_table_name']))
            plan_rows = self.plan_cur.fetchall()
            with open('plan_list.pickle', 'wb') as handle:
                pickle.dump(plan_rows, handle, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            plan_rows = pickle.load(open('plan_list.pickle','rb'))
        print("plans fetched and MAZ's sorted")
        for row in plan_rows:
            # Testing if maz is in valid defined subset
            if (row[1] in maz_dict) and (row[2] in maz_dict):
                # Actor dict is an array of  of trips
                # {"id": [{'origApn': val, 'mode': val, 'destApn': val, 'finalDepartMin':val, 'timeToSpend':val}, {...}]}
                if (row[0] not in self.actor_dict):
                    self.actor_dict[row[0]] = list()
                existing_zone = False
                orig_apn = "1"
                for item in self.actor_dict[row[0]]:
                    if (item['destAPN'] == row[1]):
                        existing_zone = True
                        # Should be leaving from the place they last went to
                        orig_apn = self.actor_dict[row[0]][-1]['apn']
                        break
                # Need to re-classify for buiness/commercial properties, and home size
                # Still a ton of work to be done
                if existing_zone == False:
                    orig_count = 0
                    while orig_apn in self.orig_apn:
                        if orig_count > 25:
                            print("Done")
                            break
                        exec_str = ("SELECT apn from {0} WHERE {1} = {2} and _ROWID_ >= (abs(random()) % (SELECT max(_ROWID_) FROM {0})) limit 1;").format(database['apn_table_name'], database['apn_selector'], row[1])
                        orig_apn = self.apn_cur.execute(exec_str)
                    self.orig_apn[orig_apn] = "Used"
                    orig_count += 1
                # Destination Generation
                dest_apn = "1"
                dest_count = 0
                while dest_apn in self.dest_apn:
                    if dest_count > 25:
                        print("Done")
                        break
                    exec_str = ("SELECT apn from {0} WHERE _ROWID_ >= (abs(random()) % (SELECT max(_ROWID_) FROM {0})) and {1} = {2} limit 1;").format(database['apn_table_name'], database['apn_selector'], row[2])
                    dest_apn = self.apn_cur.execute(exec_str)
                    dest_count += 1
                self.dest_apn[dest_apn] = "Used"
                self.actor_dict[row[0]].append({'origAPN': orig_apn, 'destAPN':dest_apn, 'mode':row[3], 'finalDepartMin':row[4], 'timeAtDest':row[5]})
        pickle.dump(self.actor_dict, open('actor_plans.pickle', 'wb'))

if __name__ == "__main__":
    x = plan_to_matplan()
    x1 = {"plan_database": "csv.db", "apn_database": "apn", "apn_table_name":"test", "apn_selector":"apn", "plan_table_name":"trips"}
    x2 = {"maz": "real_maz/maz.geojson"}
    x.from_db(x1, x2)