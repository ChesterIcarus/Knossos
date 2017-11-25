import os
import numpy as np
import xml.etree.ElementTree as et

class drive_agent(object):
    def __init__(self, time, action_type, link=None, facility=None, delay=None):
        self.time = time
        self.action_type = action_type
        self.link = link
        self.facility = facility
        self.in_vehicle = False
        if (self.facility != None):
            if (delay == "Infinity"):
                self.delay = float("inf")
            else:
                self.delay = float(delay)

class network_analysis(object):

    def __init__(self):
        print("W")


    def facility_try_loop(self, child):
        try:
            return drive_agent(child.time, child.type, facility=child.facility, delay=child.delay)
        except Exception as key_err:
            return drive_agent(child.time, child.type, child.link)

    # Simplified version of drive_agent
    # vehicle_act_profile = {"vehicle":{"exit_enter":[{"time": int, "exit_link": str, "enter_link": str}]}, repeat, again}
    # pedest_travel_obj = 
    # 
    def parse_agent_act(self, agent_xml_file_name):
        vehicle_dict = dict()
        agent_dict = dict()
        xml_tree = et.parse(agent_xml_file_name)
        xml_root = xml_tree.getroot()

        for child in root:
            try:
                if (child.vehicle in vehicle_dict.keys()):
                    vehicle_dict[child.vehicle].append(self.facility_try_loop(child))
                else:
                    vehicle_dict[child.vehicle] = [self.facility_try_loop()]

            except KeyError as key_err:
                try:
                    if (child.person in agent_dict.keys()):
                        agent_dict[child.person].append(self.facility_try_loop(child))
                    else:
                        agent_dict[child.person] = [self.facility_try_loop(child)]
                except KeyError as key_err:
                    print("Incorrect data")


    def parse_net_conf(self, net_xml_file):
