import csv
import json
from collections import defaultdict


class MagDataToPlansByPidAndMaz:
    purpose_dict = {
        "0": "h",
        "1": "w",
        "2": "w",
        "3": "w",
        "411": "w",
        "412": "w",
        "42": "w",
        "5": "s",
        "6": "w",
        "4": "w",
        "7": "s",
        "71": "s",
        "72": "s",
        "73": "s",
        "8": "s",
        "9": "s",
        "10": "s",
        "15": "w",
    }
    #     purpose_dict = {
    #     "0": "home",
    #     "1": "work",
    #     "2": "university",
    #     "3": "school",
    #     "411": "pure_escort",
    #     "412": "rideshare",
    #     "42": "other_escort",
    #     "5": "shopping",
    #     "6": "other_maintenence",
    #     "4": "breakfast",
    #     "7": "food",
    #     "71": "breakfast",
    #     "72": "lunch",
    #     "73": "dinner",
    #     "8": "visiting",
    #     "9": "other_discretionary",
    #     "10": "special_event",
    #     "15": "work_related",
    # }
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
        print("MAG Data to Plans by PID and MAZ initialized")
        self.actor_dict = defaultdict(list)
        self.cur = None
        self.conn = None
        self.table_name = None

    def read_mag_csv(self, filepath):
        '''Takes MAG data as a CSV, and parses the file to a dictionary.
        This dict is stored at a class level defaultly, and can be written
        to an output file as well.'''
        f_ = open(filepath, 'r')
        csv_reader = csv.reader(f_)
        csv_reader.__next__()
        # Per-trip Data Struture (Derived from Disaggregate trip table data dictionary.docx)
        # 0: Per household unique trip ID, 2: Household ID, 3: person number in HH
        # 19: Origin MAZ, 21: Dest. MAZ, 22: origin purpose, 23: dest. purpose, 24: Mode of travel
        # 26: Depart minute, 27: Trip dist., 29: Travel minute, 31: Arrival min, 32: Time at dest.
        # Using simplifed purpose dictionary based on config file
        for actor in csv_reader:
            # Setting negative time-at-destinations to 0
            if float(actor[32]) < 0:
                actor[32] = 0

            uuid = ('{}_{}_{}').format(
                str(actor[0]), str(actor[2]), str(actor[3]))
            pid = f'{actor[2]}_{actor[3]}'

            orig_purp = self.purpose_dict[str(actor[22])]
            dest_purp = self.purpose_dict[str(actor[23])]
            mode = self.mode_dict[str(actor[24])]

            actor_data = [uuid, pid, int(actor[19]), int(actor[21]), orig_purp, dest_purp, mode,
                          float(actor[26]), float(actor[27]), float(actor[31]), float(actor[32])]
            self.actor_dict[pid].append(actor_data)
        print("MAG data read, actors plans organized into dictionary.")

    def write_mag_to_file(self, filepath):
        print(f"Writing to {filepath}")
        with open(filepath, 'w+') as handle:
            json.dump(self.actor_dict, handle)
        print(f"Finished writing to {filepath}")


if (__name__ == "__main__"):
    example = MagDataToPlansByPidAndMaz()
    example.read_mag_csv("raw/output_disaggTripList.csv")
    example.write_mag_to_file("data/mag_agents_w_APN.json")
