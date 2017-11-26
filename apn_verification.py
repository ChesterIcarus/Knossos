from shapely.geometry import shape, asShape
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
        # maz_set = gp.read_file(filepath['maz'])
        # maz_file.close()
        print("Maz Loaded")
        parcel_file = open(filepath['parcel'], 'r')
        parcel_set = json.load(parcel_file)
        # parcel_set = gp.read_file(filepath['parcel'])
        # parcel_file.close()
        print("Parcel Loaded")
        conn = pymysql.connect(host=database['host'], user=database['user'], password=database['password'], database=database['database'])
        cur = conn.cursor()
        if (database['drop'] == True):
            cur.execute(("DROP TABLE if exists {}").format(database['table_name']))
            exec_str = ("""CREATE table {0} (apn int unsigned, maz int unsigned)""").format(database['table_name'])
            cur.execute(exec_str)
        else:
            if (database['drop'] == True):
                cur.execute(("DROP TABLE {0};").format(database['table_name']))
                exec_str = ("CREATE table {0} (apn INT UNSIGNED NOT NULL PRIMARY KEY, maz INT UNSIGNED NOT NULL, usage TINYINT UNSIGNED NOT NULL)").format(database['table_name'])
                cur.execute(exec_str)
        insert_tuple = list()
        progress = 0
        total = len(maz_set['features'])
        for bounding in maz_set['features']:
            bounding_shape = shape(bounding['geometry'])
            if (progress >= (.025*total)):print(".25")
            if (progress >= (.1*total)):print("1");conn.commit()
            if (progress >= (.2*total)):print("2");conn.commit()
            if (progress >= (.3*total)):print("3");conn.commit()
            if (progress >= (.4*total)):print("4");conn.commit()
            if (progress >= (.5*total)):print("5");conn.commit()
            if (progress >= (.6*total)):print("6");conn.commit()
            if (progress >= (.7*total)):print("7");conn.commit()
            if (progress >= (.8*total)):print("8");conn.commit()
            if (progress >= (.9*total)):print("9");conn.commit()
            progress += 1
            for feature in parcel_set['features']:
                feature_shape = shape(feature['geometry'])
                if feature_shape.within(bounding_shape):
                    insert_tuple.append(tuple([feature['properties']['APN'], bounding['properties']['MAZ_ID_10'], 0]))
                    parcel_set['features'].remove(feature)
                    break
        exec_str = ("INSERT into {0} (apn, maz) values ({1}, {1});").format(database['table_name'], insert_tuple)
        cur.executemany(exec_str)
        conn.commit()
        conn.close()


    def testing(self, filepath):
        f_ = pygeoj.load(filepath)
        print(f_.crs)
        print(f_.bbox)
        print(f_[2])

if __name__ == "__main__":
    x = apn_verification()
    files = {'parcel': 'Parcels_All/all_parcel.geojson', 'maz':'real_maz/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'host':'localhost', 'user':'root', 'password':pw, 'database':'apn', 'table_name':'test', 'drop':True}
    x.parsing_apns(files, db_param)

