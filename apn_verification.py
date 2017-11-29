from shapely.geometry import shape, polygon
from __init__ import bounding_for_maz
import shapely
import pymysql
import getpass
import json
import sqlite3 as sql

class apn_verification(object):
    def __init__(self):
        print('APN Verification started')


    def parsing_apns(self, filepath, database):
        # host, user, password, database, table_name, drop
        maz_file = open(filepath['maz'], 'r')
        maz_set = json.load(maz_file)
        maz_file.close()
        print("Maz Loaded")

        parcel_file = open(filepath['parcel'], 'r')
        parcel_set = json.load(parcel_file)
        parcel_file.close()
        print("Parcel Loaded")
        try:
            conn = pymysql.connect(host=database['host'], user=database['user'], password=database['password'], database=database['database'])
            print("System Level DB created")
        except KeyError as keyErr:
            print("File Level DB created")
            conn = sql.connect(database['database'])
        cur = conn.cursor()
        if (database['drop'] == True):
            cur.execute(("DROP TABLE if exists {}").format(database['table_name']))
            exec_str = ("CREATE table {0} (apn CHAR(9), maz INT UNSIGNED NOT NULL)").format(database['table_name'])
            cur.execute(exec_str)
        # else:
        #     if (database['drop'] == True):
        #         cur.execute(("DROP TABLE {0};").format(database['table_name']))
        #         exec_str = ("CREATE table {0} (apn INT UNSIGNED NOT NULL PRIMARY KEY, maz INT UNSIGNED NOT NULL, usage TINYINT UNSIGNED NOT NULL)").format(database['table_name'])
        #         cur.execute(exec_str)
        insert_tuple = list()
        progress = 0
        total = len(parcel_set['features'])
        # bounding_for_maz = polygon.LinearRing([(564413, 892531), (618474, 892531), (564413, 894696), (618474, 894696)])
        for maz in maz_set['features']:
            try:
                if not ((shape(maz['geometry'])).representative_point()).within(bounding_for_maz):
                    maz_set['features'].remove(maz)
            except ValueError as topErr:
                if not (shape(maz['geometry'])).within(bounding_for_maz):
                    maz_set['features'].remove(maz)
        for feature in parcel_set['features']:
            try:
                feature_shape = (shape(feature['geometry'])).representative_point()
            except ValueError as topExcep:
                feature_shape = shape(feature['geometry'])
            progress += 1
            if feature_shape.within(bounding_for_maz):
                for bounding in maz_set['features']:
                    bounding_shape = shape(bounding['geometry'])
                    if feature_shape.within(bounding_shape):
                        insert_tuple = tuple([feature['properties']['APN'], bounding['properties']['MAZ_ID_10']])
                        print(insert_tuple)
                        exec_str = ("INSERT into {0} values {1};").format(database['table_name'], insert_tuple)
                        cur.execute(exec_str)
                        # parcel_set['features'].remove(feature)
                        break
        conn.commit()
        conn.close()

if __name__ == "__main__":
    x = apn_verification()
    files = {'parcel': 'Parcels_All/all_parcel.geojson', 'maz':'real_maz/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'user':'root', 'database':'apn.db', 'table_name':'bounded_maz', 'drop':True, 'host':'localhost'}
    x.parsing_apns(files, db_param)

