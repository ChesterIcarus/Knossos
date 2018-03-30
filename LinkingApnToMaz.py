import json
import pyproj
import getpass
import sqlite3 as sql
import MySQLdb as mysql
from functools import partial
from shapely.ops import transform
from shapely.geometry import shape, polygon, LineString

class LinkingApnToMaz:
    def __init__(self):
        print('APN\'s linked to MAZ started (NOT USING OSMID\'s)')
        self.apn_maz = dict()
        self.maz_set = None
        self.parcel_set = None
        self.crs = None
        self.conn = None
        self.cur = None
        self.db_name = None
        self.table_name = None
        self.bounding_for_maz = None
        self.bounded_maz_set = None
        self.bounded_maz_shapes = list()
        self.bounded_eval = False
        self.db_insert = list()

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
        self.db_name = database['db']
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
            self.conn.commit()

    def set_bounding(self, filepath):
        '''Allows the user to specify a subsection of the entered area to evaluate'''
        self.bounded_eval = True
        with open(filepath, 'r') as handle:
            data = json.load(handle)
        print("Setting boundries for evaluations")
        epsg_2223 = pyproj.Proj('+proj=tmerc +lat_0=31 +lon_0=-111.9166666666667'+
                                    ' +k=0.9999 +x_0=213360 +y_0=0 +ellps=GRS80 '+
                                    '+towgs84=0,0,0,0,0,0,0 +units=ft +no_defs')
        epsg_4326 = pyproj.Proj(init='epsg:4326')
        converted_data = list()

        for point in data['geometries'][0]['coordinates']:
            converted_data.append(pyproj.transform(epsg_4326, epsg_2223, point[0], point[1]))

        self.bounding_for_maz = polygon.Polygon(converted_data)
        print("Boundries set!")

    def find_maz_in_bounds(self):
        print("Finding MAZ in bounds")
        self.bounded_maz_set = {'features': list()}
        for maz in self.maz_set['features']:
            temp_shape = shape(maz['geometry'])
            try:
                temp_point = temp_shape.representative_point()
            except ValueError:
                temp_point = temp_shape
            if temp_point.within(self.bounding_for_maz):
                self.bounded_maz_set['features'].append(maz)
        print("Found MAZ\'s in bounds")

    def create_maz_shape_list(self, maz_list):
        ret_list = list()
        for maz in maz_list['features']:
            ret_list.append(tuple([shape(maz['geometry']), maz['properties']['MAZ_ID_10']]))
        return ret_list

    def assign_maz_per_apn(self, write_to_database=False):
        # "Meat" of the module, connection MAZ, APN, and osm_id
        # This creates the output to be used in agent plan generation
        print("Assigning MAZ per APN")
        print(f"There are {len(self.parcel_set['features'])} total features")
        maz_shape_list = self.create_maz_shape_list(self.bounded_maz_set)
        print(f"There are {len(maz_shape_list)} MAZ\'s")

        for feature in self.parcel_set['features']:
            temp_shape = shape(feature['geometry'])
            try:
                temp_point = temp_shape.representative_point()
            except (TypeError, ValueError):
                temp_point = temp_shape
            if temp_point.within(self.bounding_for_maz):
                for maz in maz_shape_list:
                    if temp_point.within(maz[0]):
                        self.db_insert.append(tuple([feature['geometry']['coordinates'][0],
                                                    feature['geometry']['coordinates'][1],
                                                    feature['properties']['APN'],
                                                    maz[1]]))

        if write_to_database is True:
            print("Writing to database")
            print(len(self.db_insert))
            self.cur.executemany(f"INSERT INTO {self.db_name}.{self.table_name} values (%s,%s,%s,%s)", self.db_insert)
            self.conn.commit()
            self.conn.close()
        print("MAZ\'s assigned")

if __name__ == "__main__":
    files = {'parcel': 'Parcels_All/all_parcel.geojson', 'maz':'MAZ/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'user':'root', 'db':'LinkingApnToMaz', 'host':'localhost', 'password': pw}

    example = LinkingApnToMaz()
    example.load_maz(files['maz'])
    example.load_parcel(files['parcel'])
    example.set_crs_from_parcel()
    example.connect_database(db_param, table_name="Example", drop=True)
    # 258710.1067, 122857.2981, 1157943.9948, 2186600.2033
    # full_ariz = [
    #     (291681.866638, 2147002.203025),
    #     (1114836.32474, 2099088.372318),
    #     (1092055.717785, 913579.224235),
    #     (341912.702254, 946252.321431)]
    # bounding_coords = {'poly_coords': full_ariz, 'poly_crs': 'epsg:2223'}
    example.set_bounding('maricopa_poly.geojson')
    example.find_maz_in_bounds()
    example.assign_maz_per_apn(write_to_database=True)
