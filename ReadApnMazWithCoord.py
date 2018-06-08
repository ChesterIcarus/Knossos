import json


class ReadApnMazWithCoord:
    def __init__(self, filepath=None):
        self.FILEPATH = filepath
        self.apn_by_maz_w_coord = list()

    def process_file(self, filepath=None):
        if filepath is None:
            if self.FILEPATH is not None:
                filepath = self.FILEPATH

        with open(filepath, 'r') as handle:
            f_data = handle.readlines()

        for line in f_data[1:-1]:
            tmp = line.strip().split(',')
            apn = tmp[2]
            x = tmp[-2]
            y = tmp[-1]
            maz = tmp[11]
            self.apn_by_maz_w_coord.append(tuple([apn, maz, x, y]))
        print(f'Found {len(self.apn_by_maz_w_coord)} APN\'s')

    def write_output(self, filepath):
        with open(f'{filepath}.json', 'w+') as handle:
            json.dump(self.apn_by_maz_w_coord, handle, indent=1)
        with open(f'{filepath}_dict_APN.json', 'w+') as handle:
            tmp = {x[0]: x[1:4] for x in self.apn_by_maz_w_coord}
            json.dump(tmp, handle, indent=1)
        with open(f'{filepath}_dict_MAZ.json', 'w+') as handle:
            tmp = {x[1]: [x[0], x[2], x[3]] for x in self.apn_by_maz_w_coord}
            json.dump(tmp, handle, indent=1)


if __name__ == '__main__':
    example = ReadApnMazWithCoord('../Data/APN_MAZ_Coord.txt')
    example.process_file()
    example.write_output('full_maricopa_parcel_w_coord')
