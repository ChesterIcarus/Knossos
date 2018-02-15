from shapely.geometry import shape, polygon
from __init__ import bounding_for_maz
import json
import pyproj
import pymysql
import getpass
import sqlite3 as sql
import pymysql as mysql
from functools import partial
from shapely.ops import transform


class LinkingApnToMaz:
    def __init__(self):
        print('APN\'s linked to MAZ started (NOT USING OSMID\'s)')
        self.apn_maz = dict()
        self.maz_set = None
        self.parcel_set = None
        self.crs = None
        self.conn = None
        self.cur = None
        self.table_name = None
        self.bounding_for_maz = None
        self.bounded_maz_set = None
        self.bounded_eval = False

    # Loading maz data for rest of class data
    def load_maz(self, filepath):
        maz_file = open(filepath, 'r')
        self.maz_set = json.load(maz_file)
        maz_file.close()
        print("Maz Loaded")

    # Loading parcel data for rest of class data
    def load_parcel(self, filepath):
        parcel_file = open(filepath, 'r')
        self.parcel_set = json.load(parcel_file)
        parcel_file.close()

    def set_crs_from_parcel(self):
        try:
            self.crs = (self.parcel_set['crs']['properties']['name']).split(':')[-1]
        except (KeyError, IndexError) as err:
            print("Invalid GeoJSON for Parcel Set")
            raise err
        print("Parcel Loaded with CRS:" + str(self.crs))

    def connect_database(self, database, table_name, drop):
        '''Connecting to Database used to store the output, if the output is to be written to a database.
        Input is of the form: database = 
            {'host': port, 'user': Valid Database User,
            'password': Password for the specified user, 'db': Database to use},
            table_name =  Name of the table to use (Must exist if drop is False),
            drop =  Boolean indicating if we are to drop existing table='tablename
        Output is stored in the form: (X Coordinate, Y Coordinate, APN Identifier, MAZ Identifier)'''

        self.table_name = table_name
        try:
            self.conn = mysql.connect(**database)
            print("System Level DB created")
        except KeyError:
            self.conn = sql.connect(database['database'])
            print("File Level DB created")
        self.cur = self.conn.cursor()
        if drop is True:
            self.cur.execute(("DROP TABLE if exists {}").format(table_name))
            exec_str = ("CREATE table {0} (coordX FLOAT, coordY FLOAT, APN CHAR(12), maz INT UNSIGNED NOT NULL)").format(table_name)
            self.cur.execute(exec_str)

    def set_bounding(self, apn_bounding=None):
        '''Allows the user to specify a subsection of the entered area to evaluate'''
        # TODO: Set default such that it includes all of United States
        self.bounded_eval = True
        if apn_bounding:
            bounding_from_inp = polygon.Polygon(apn_bounding['poly_coords'], apn_bounding['poly_holes'] if ('poly_holes' in apn_bounding.keys()) else None)
            proj_to_map = partial(pyproj.transform, pyproj.Proj(init=apn_bounding['poly_crs']), pyproj.Proj(init='epsg:{}'.format(self.crs)))
            bounding_for_maz = transform(proj_to_map, bounding_from_inp)

        else:
            default_bounding = polygon.Polygon([(649054, 896498), (649192, 888749), (665535, 896129), (663998, 889026)])
            proj_to_map = partial(pyproj.transform, pyproj.Proj(init='epsg:2223'), pyproj.Proj(init='epsg:{}'.format(self.crs)))
            bounding_for_maz = transform(proj_to_map, default_bounding)
        self.bounding_for_maz = bounding_for_maz

    def find_maz_in_bounds(self):
        self.bounded_maz_set = {'features': list()}
        for maz in self.maz_set['features']:
            temp_shape = shape(maz['geometry'])
            try:
                temp_point = temp_shape.representative_point()
            except ValueError:
                temp_point = temp_shape
            if temp_point.within(self.bounding_for_maz):
                self.bounded_maz_set['features'].append(maz)

    def assign_maz_per_apn(self, write_to_database=False):
        # "Meat" of the module, connection MAZ, APN, and osm_id
        # This creates the output to be used in agent plan generation
        if self.bounded_eval:
            maz_set_local = self.bounded_maz_set
        else:
            maz_set_local = self.maz_set

        for feature in self.parcel_set['features']:
            temp_shape = shape(feature['geometry'])
            try:
                temp_point = temp_shape.representative_point()
            except (TypeError, ValueError):
                try:
                    temp_point = temp_shape
                except TypeError:
                    pass
            if temp_point.within(self.bounding_for_maz) or (self.bounded_eval is False):
                for bounding in maz_set_local['features']:
                    bounding_shape = shape(bounding['geometry'])
                    if temp_point.within(bounding_shape):
                        insert_tuple = tuple([feature['geometry']['coordinates'][0],
                                            feature['geometry']['coordinates'][1],
                                            feature['properties']['APN'],
                                            bounding['properties']['MAZ_ID_10']])
                        if write_to_database is True:
                            exec_str = ("INSERT INTO {} values {};").format(self.table_name, insert_tuple)
                            self.cur.execute(exec_str)
                        if bounding['properties']['MAZ_ID_10'] in self.apn_maz:
                            self.apn_maz[bounding['properties']['MAZ_ID_10']].append(bounding)
                        else:
                            self.apn_maz[bounding['properties']['MAZ_ID_10']] = [list(insert_tuple)]

        if write_to_database is True:
            self.conn.commit()
            self.conn.close()


if __name__ == "__main__":
    # files = {'parcel': './Parcel/parcel.geojson', 'maz': 'MAZ/maz.geojson', 'osm': '../Shapefiles/Cleaned/working_test.xml'}
    files = {'parcel': '../Shapefiles/Cleaned/test_dirty_point.geojson', 'maz':'MAZ/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'user':'root', 'db':'LinkingApnToMaz', 'host':'localhost', 'password': pw}

    example = LinkingApnToMaz()
    example.load_maz(files['maz'])
    example.load_parcel(files['parcel'])
    example.set_crs_from_parcel()
    example.connect_database(db_param, table_name="Example", drop=True)
    example.set_bounding()
    example.find_maz_in_bounds()
    example.assign_maz_per_apn(True)