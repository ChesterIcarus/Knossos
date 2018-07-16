import json
import csv
from collections import defaultdict


class ReadApnMazWithCoord:
    def __init__(self, filepath=None):
        self.FILEPATH = filepath
        self.apn_by_maz_w_coord = list()

    def process_file(self, filepath=None):
        if filepath is None:
            if self.FILEPATH is not None:
                filepath = self.FILEPATH

        with open(filepath, 'r') as handle:
            f_data = csv.reader(handle)
            next(f_data)
            for tmp in f_data:
                apn = tmp[2]
                x = float(tmp[-2])
                y = float(tmp[-1])

                self.apn_by_maz_w_coord.append(
                    [apn, int(tmp[1]), x, y, int(tmp[10]), int(tmp[11])])
        print(f'Found {len(self.apn_by_maz_w_coord)} APN\'s')

    def write_comprehensive_data(self, filepath):
        with open(f'{filepath}.json', 'w+') as handle:
            json.dump(self.apn_by_maz_w_coord, handle, indent=1)
        with open(f'{filepath}DictAPN.json', 'w+') as handle:
            tmp = {x[0]: x[1:4] for x in self.apn_by_maz_w_coord}
            json.dump(tmp, handle, indent=1)
        all_apn_for_maz = defaultdict(list)
        for parcel in self.apn_by_maz_w_coord:
            all_apn_for_maz[parcel[1]].append(
                tuple([parcel[0], parcel[2], parcel[3]]))
            all_apn_for_maz[parcel[4]].append(
                tuple([parcel[0], parcel[2], parcel[3]]))
            all_apn_for_maz[parcel[5]].append(
                tuple([parcel[0], parcel[2], parcel[3]]))
        for maz in list(all_apn_for_maz):
            all_apn_for_maz[maz] = tuple(set(tuple(all_apn_for_maz[maz])))
        with open(f'{filepath}DictMAZ.json', 'w+') as handle:
            json.dump(all_apn_for_maz, handle, indent=1)


if __name__ == '__main__':
    example = ReadApnMazWithCoord('Data/APN_MAZ_Coord.txt')
    example.process_file()
    example.write_comprehensive_data(
        'CleanedApnMazMappings/fullMaricopaParcelCoord')
