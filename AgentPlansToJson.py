import json
from collections import defaultdict
import math


class AgentPlansToJson:
    def __init__(self):
        print("Converting agents plans from CSV to JSON")
        self.csv_plans = list()
        self.plan_dict = defaultdict(list)

    def read_csv_plans(self, filename):
        with open(filename, 'r') as handle:
            for line in handle.readlines():
                self.csv_plans.append(tuple((line.strip()).split(',')))

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
        for plan in self.csv_plans:
            time_sec = int(math.floor(float(plan[10]))) + 16200
            time_string = self.seconds_to_str(time_sec)

            self.plan_dict[plan[0]].append({
                "to_sort": int(math.floor(float(plan[10]))),
                "edge": {
                    "type": plan[8].strip(),
                    "x": plan[3].strip(),
                    "y": plan[4].strip(),
                    "end_time": time_string,
                    "mode": plan[7].strip()
                }
            })

        for actor_id in list(self.plan_dict):
            sorted(self.plan_dict[actor_id], key=lambda x: x["to_sort"])
            # home_act =
            # self.plan_dict[actor_id] = s

    def write_json(self, filename):
        with open(filename, "w+") as handle:
            json.dump(self.plan_dict, handle, indent=4)


if __name__ == "__main__":
    example = AgentPlansToJson()
    example.read_csv_plans("actor_plans_apn_coord_5_22_18_ROUND_1.txt")
    example.to_dict()
    example.write_json("actor_plans_apn_coord.json")
