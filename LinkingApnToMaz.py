import json
import geojson
import pyproj
import getpass
import pymysql as sql
from functools import partial
import geopandas as gpd
# from shapely.wkt import load
import shapely.wkt as wkt
from shapely.ops import transform
from shapely.geometry import shape, polygon, LineString, mapping
import shapely


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
            self.crs = (self.parcel_set['crs']
                        ['properties']['name']).split(':')[-1]
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
            self.conn = sql.connect(**database)
            print("System Level DB created")
        except KeyError:
            self.conn = sql.connect(db=database['database'])
            print("File Level DB created")
        self.cur = self.conn.cursor()
        if drop is True:
            self.cur.execute(("DROP TABLE if exists {}").format(table_name))
            exec_str = ("CREATE table {0} (coordX FLOAT, coordY FLOAT, APN CHAR(12), maz INT UNSIGNED NOT NULL)").format(
                table_name)
            self.cur.execute(exec_str)
            self.conn.commit()

    def set_bounding(self, geojson_filepath=None, geojson_crs=None):
        '''Allows the user to specify a subsection of the area to evaluate.
            This subsection will be identified by MAZ ID's.'''
        if (geojson_filepath is not None) and (geojson_crs is not None):
            json_obj = None
            with open(geojson_filepath, 'r', encoding="ISO-8859-1") as handle:
                json_obj = json.load(handle)
            for feature in json_obj['features']:
                if feature['properties']['NAME'] == "Maricopa":
                    json_obj['features'] = [feature]
                    break

            shp = shapely.geometry.shape(json_obj['features'][0]['geometry'])
            tmp = gpd.GeoSeries([shp])
            tmp.crs = {'init': "epsg:4326"}
            tmp_w_update_crs = tmp.to_crs({'init': 'epsg:2223'})
            bounding_for_maz = tmp_w_update_crs[0]

        else:
            default_obj = {
                'type': 'Polygon',
                'coordinates': [[[649054, 896498], [649192, 888749], [665535, 896129], [663998, 889026]]]}
            default_shp = shapely.geometry.shape(default_obj)
            tmp = gpd.GeoSeries([default_shp])
            tmp.crs = {'init': 'epsg:2223'}
            bounding_for_maz = tmp.to_crs({'init': f'epsg:{self.crs}'})[0]
        self.bounding_for_maz = bounding_for_maz

        print("Boundries set!")
        return bounding_for_maz

    def find_maz_in_bounds(self):
        print("Finding MAZ in bounds")
        self.bounded_maz_set = {'features': list()}
        print(
            f"There are {len(self.maz_set['features'])} MAZ\'s before boundry operations")
        for maz in self.maz_set['features']:
            temp_shape = shape(maz['geometry'])
            temp_point = temp_shape.representative_point()
            if temp_point.within(self.bounding_for_maz):
                self.bounded_maz_set['features'].append(maz)
        print(
            f"Found {len(self.bounded_maz_set['features'])} MAZ\'s in bounds")

    def create_maz_shape_list(self, maz_list):
        ret_list = list()
        for maz in maz_list['features']:
            ret_list.append(
                tuple([shape(maz['geometry']), maz['properties']['MAZ_ID_10']]))
        return ret_list

    def assign_maz_per_apn(self, write_to_database=False, write_json=False, json_path="MAZ_by_APN.json", maz_bounds_json=False, maz_bounds_path="valid_maz_for_bounds.json"):
        # "Meat" of the module, connection MAZ, APN, and osm_id
        # This creates the output to be used in agent plan generation
        print("Assigning MAZ per APN")
        print(f"There are {len(self.parcel_set['features'])} total features")
        maz_shape_list = self.create_maz_shape_list(self.bounded_maz_set)
        if maz_bounds_json:
            with open(maz_bounds_path, 'w+') as handle:
                try:
                    tmp = [mapping(f[0].geom) for f in maz_shape_list]
                    geojson.dump(tmp, handle)
                    print(f'JSON object wrote to {maz_bounds_path}')
                except Exception as e_:
                    print(f'Unable to write JSON:\n{e_}')
        print(f"There are {len(maz_shape_list)} MAZ\'s")
        temp_point = None
        temp_shape = None
        pause_on_execp = True
        try:
            for feature in self.parcel_set['features']:
                temp_shape = shape(feature['geometry'])
                try:
                    temp_point = temp_shape.representative_point()
                except (TypeError, ValueError):
                    temp_point = temp_shape
                try:
                    if temp_point.within(self.bounding_for_maz):
                        for maz in maz_shape_list:
                            if temp_point.within(maz[0]):
                                self.db_insert.append(tuple([temp_point.x,
                                                             temp_point.y,
                                                             feature['properties']['APN'],
                                                             maz[1]]))
                except Exception as general_ex:
                    if pause_on_execp:
                        print(general_ex)
                        resp = input(
                            'Pause for future exceptions? y/n ("y" if you would like the option to write the current data to a JSON file)\n')
                        if resp == "n":
                            pause_on_execp = False
                        else:
                            resp = input(
                                "Would you like to write the data to a JSON file? y/n\n")
                            if resp == 'y':
                                f_name = input(
                                    "Please enter entire filename (eg. 'test.json')\n")
                                with open(f_name, 'w+') as handle:
                                    try:
                                        geojson.dump({
                                            "db_insert": self.db_insert,
                                            "temp_point": temp_point if (temp_point is not None) else None,
                                            "temp_shape": temp_shape if (temp_shape is not None) else None},
                                            handle)
                                    except Exception as file_exep:
                                        print(
                                            f'Unable to write to file due to:\n{file_exep}')
        except KeyboardInterrupt as kb_int:
            resp = input(
                "Would you like to write the data to a JSON file? y/n\n")
            if resp == 'y':
                f_name = input(
                    "Please enter entire filename (eg. 'test.json')\n")
                with open(f_name, 'w+') as handle:
                    try:
                        geojson.dump({
                            "db_insert": self.db_insert,
                            "temp_point": temp_point if (temp_point is not None) else None,
                            "temp_shape": temp_shape if (temp_shape is not None) else None},
                            handle)
                    except Exception as file_exep:
                        print(
                            f'Unable to write to file due to:\n{file_exep}')
        if write_to_database is True:
            print(
                f"Writing {len(self.db_insert)} valid MAZ - APN relations to database")
            self.cur.executemany(
                f"INSERT INTO {self.db_name}.{self.table_name} values (%s,%s,%s,%s)", self.db_insert)
            self.conn.commit()
            self.conn.close()
        if write_json:
            with open(json_path, 'w+') as handle:
                try:
                    try:
                        json.dump(self.db_insert, handle)
                    except Exception as excep_2:
                        geojson.dump(self.db_insert, handle)
                except Exception as excep_1:
                    print(
                        f"Unable to write final MAZ - APN relations to file: {json_path} due to:\n{excep_1}")

        print("MAZ\'s assigned")


if __name__ == "__main__":
    files = {'parcel': '../Data/parcel.geojson', 'maz': '../Data/maz.geojson'}
    # pw = getpass.getpass()
    db_param = {'user': 'root', 'db': 'linkingApnToMaz',
                'host': 'localhost', 'password': 'Am1chne>'}

    example = LinkingApnToMaz()
    example.connect_database(db_param, table_name="FullArizona", drop=True)
    example.load_maz(files['maz'])
    example.load_parcel(files['parcel'])
    example.set_crs_from_parcel()
    # example.set_bounding()
    example.set_bounding(
        geojson_filepath='../Data/gz_2010_us_050_00_5m.json', geojson_crs='epsg:4326')
    example.find_maz_in_bounds()
    example.assign_maz_per_apn(
        write_to_database=True, write_json=True, interim_json=True)
