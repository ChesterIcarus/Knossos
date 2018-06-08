import json
from collections import defaultdict
import random
import math


class AgentPlansToJson:
    def __init__(self):
        print("Converting agents plans from CSV to JSON")
        self.mode_dict = {
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
        self.csv_plans = list()
        self.maz_plan_dict = None
        self.apn_per_agent = defaultdict(dict)
        self.apn_plan_dict = defaultdict(list)

    def read_json_maz_plans(self, filename):
        with open(filename, 'r') as handle:
            self.maz_plan_dict = json.load(handle)

    def assign_apn_to_agents(self, maz_to_apn_filepath):
        with open(maz_to_apn_filepath, 'r') as handle:
            maz_apn_map = json.load(handle)

        for agent in list(self.maz_plan_dict):
            tmp_hash_list = list()
            for trip in self.maz_plan_dict[agent]:
                # trip[2] is origin MAZ, trip[4] is origin purpose
                # trip[3] is destination MAZ, trip[5] is destination purpose
                tmp_hash_list.append(tuple([trip[2], trip[4]]))
                tmp_hash_list.append(tuple([trip[3], trip[5]]))
            distinct_dest = set(tmp_hash_list)
            # For each distict destination (rudimentery analysis currently) assign a single APN
            # This is mapped to the tuple of MAZ/dest combo, which is currently
            # assumed to be unique per agent
            for dest in list(distinct_dest):
                self.apn_per_agent[agent][dest] = random.sample(
                    maz_apn_map[dest[0]], 1)[0]

    def seconds_to_str(self, seconds):
        hours = str(math.floor(seconds / (60 * 60)))
        seconds -= int(hours) * 60 * 60
        minutes = str(math.floor(seconds / 60))
        seconds -= int(minutes) * 60
        seconds = str(seconds)
        time_list = [hours, minutes, seconds]
        for index in range(0, len(time_list)):
            while len(time_list[index]) < 2:
                time_list[index] = f"0{time_list[index]}"
        return f"{time_list[0]}:{time_list[1]}:{time_list[2]}"

    def to_dict(self):
        for agent in list(self.maz_plan_dict):
            for trip in self.maz_plan_dict[agent]:
                # Convert time from seconds offset to proper string for used in MATsim
                # time_sec = int(math.floor(float(trip[10]))) + 16200
                depart_time_sec = (float(trip[7]) * 60) + 16200
                depart_time_str = self.seconds_to_str(depart_time_sec)
                arrival_time_sec = (float(trip[9]) * 60) + 16200
                arrival_time_str = self.seconds_to_str(arrival_time_sec)
                # Finding the apn's based off the unique id tuple from @assign_apn_to_agents
                apn_uid_tup_orig = tuple([trip[2], trip[4]])
                apn_uid_tup_dest = tuple([trip[3], trip[5]])
                orig_apn = self.apn_per_agent[agent][apn_uid_tup_orig]
                dest_apn = self.apn_per_agent[agent][apn_uid_tup_dest]

                self.apn_plan_dict[agent].append({
                    "to_sort": depart_time_sec,
                    "mode": self.mode_dict[trip[6]],
                    "depart_time": depart_time_str,
                    "arrival_time": arrival_time_str,
                    "orig": {
                        "x": float(orig_apn[1]),
                        "y": float(orig_apn[2]),
                        "purpose": trip[4]
                    },
                    "dest": {
                        "x": float(dest_apn[1]),
                        "y": float(dest_apn[2]),
                        "purpose": trip[5]
                    }
                })

        for agent in list(self.apn_plan_dict):
            sorted(self.apn_plan_dict[agent], key=lambda x: x["to_sort"])

    def add_home_act(self):
        print('')
        # home_act =
        # self.plan_dict[actor_id] = s

    def write_json(self, filename):
        with open(filename, "w+") as handle:
            json.dump(self.maz_plan_dict, handle, indent=1)


if __name__ == "__main__":
    example = AgentPlansToJson()
    example.read_json_maz_plans(
        'Data/MagDataToPlan_output_Example_no_indent.json')
    example.to_dict()
    example.write_json("actor_plans_apn_coord.json")
