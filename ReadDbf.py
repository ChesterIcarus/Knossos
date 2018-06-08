import dbfread
import json
import geojson
from shapely.geometry import shape


class ReadDbf:
    def __init__(self, dbf_file=None):
        print("init")
        self.DBF_FILE = dbf_file
        self.apn_by_maz = list()
        # apn index: 1, MAZ index (if there): 10

    def process_dbf(self, dbf_file=None):
        if self.DBF_FILE is None:
            if dbf_file is None:
                raise ValueError('Need path to DBF File')
            else:
                self.DBF_FILE = dbf_file
        missing_count = 0
        for parcel in dbfread.DBF(self.DBF_FILE):
            if parcel['ADDRESS'] != '' and parcel['MAZ_ID_10'] and parcel['APN']:
                self.apn_by_maz.append(
                    tuple([tuple([parcel['Shape_Leng'], parcel['Shape_Area']]), parcel['MAZ_ID_10'], parcel['APN']]))
            else:
                missing_count += 1
        print(f'{len(self.apn_by_maz)} well-formed parcel entries, {missing_count} malformed parcel entries')

    def add_coord_per_apn(self, coord_filepath):
        with open(coord_filepath, 'r') as handle:
            raw_coord_data = geojson.load(handle)
        coord_data = dict()
        for x in raw_coord_data['features']:
            try:
                point = shape(x['properties']['geometry']
                              ).representative_point()
                # coord_data[x["properties"]["APN"]] = point
                coord_data[tuple([x["properties"]["Shape_Leng"],
                                  x["properties"]["Shape_Area"]])] = point
            except KeyError:
                continue
        tmp_list = list()
        for index, parcel in enumerate(self.apn_by_maz):
            try:
                tmp_list.append(tuple(
                    [coord_data[parcel[0]].x, coord_data[parcel[0]].y, parcel[2], parcel[1]]))
            except KeyError:
                print(parcel)
                input()
                # continue
        self.apn_by_maz = tmp_list

    def read_apn_by_maz_wo_coord_file(self, filepath):
        with open(filepath, 'r') as handle:
            self.apn_by_maz = json.load(handle)

    def write_to_file(self, filename):
        print(f'Writing to {filename}')
        print(f'{len(self.apn_by_maz)} final APN-MAZ relations with coordinates')
        with open(filename, 'w+') as handle:
            json.dump(self.apn_by_maz, handle)


if __name__ == "__main__":
    example = ReadDbf('APN_MAZ.dbf')
    # example = ReadDbf()
    # example.read_apn_by_maz_wo_coord_file('apn_by_maz.json')
    example.process_dbf()
    example.add_coord_per_apn('../Data/parcel.geojson')
    example.write_to_file('apn_by_maz_w_coords.json')
