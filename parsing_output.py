import xml.etree.ElementTree as et
from xml.dom import minidom
from collections import defaultdict

type_dict = {"all": ["time", "type"],
             "actend": ["actType", "link", "person"],
             "departure": ["legMode", "link", "person"],
             "PersonEntersVehicle": ["vehicle", "person"],
             "vehicle enters traffic": ["vehicle", "networkMode", "relativePosition", "link", "person"],
             "vehicle leaves traffic": ["vehicle", "networkMode", "relativePosition", "link", "person"],
             "PersonLeavesVehicle": ["vehicle", "person"],
             "arrival": ["legMode", "link", "person"],
             "actstart": ["actType", "link", "person"],
             "entered link": ["vehicle", "link"],
             "left link": ["vehicle", "link"]
                 }


class outputParser(object):
    def __init__(self):
        # Init self var's
        print("Starting output parsing")
        self.events_of_interest = None
        self.event_dict = defaultdict(list)
        self.events = None
        self.db_conn = None
        self.db_cur = No

    def input_file(self, filename):
        try:
            if filename.split(".")[-1] != "xml":
                raise ValueError("Require XML file")
        except IndexError as idx:
            raise ValueError("Invalid filename")

        event_xml = minidom.parse(filename)
        self.events = event_xml.getElementsByTagName("event")

    def process_all_events(self):
        for event in self.events:
            type_list = type_dict[event.attributes["type"].value]
            _ev = []
            try:
                for attr in (type_dict["all"] + type_list):
                    _ev.append(event.attributes[attr].value)
                self.event_dict[event.attributes["type"].value].append(_ev)
            except KeyError as key_err:
                print(event.attributes["type"].value)
                input()

    def process_specif_events(self):
        for event in self.events:
            if event.attributes["type"].value in self.events_of_interest:
                type_list = type_dict[event.attributes["type"].value]
                _ev = []
                for attr in (type_dict["all"] + type_list):
                    _ev.append(event.attributes[attr].value)
                self.event_dict[event.attributes["type"].value].append(_ev)

    def parse_matsim_output(self):
        if self.events_of_interest is None:
            self.process_all_events()
        else:
            self.process_specif_events()

    def write_to_group(self, filename):
        try:
            if filename.split(".")[-1] != "txt":
                raise ValueError("Requires CSV file")
        except IndexError as idx:
            raise ValueError("Invalid Filename")
        for key, value in self.event_dict.items():
            with open(filename.format(key), 'w+') as handle:
                handle.write((('{}, ' * (len(type_dict["all"] + type_dict[key]))).\
                        format(*(type_dict["all"] + type_dict[key]))).rstrip(', ') + "\n")
                for event in value:
                    _ev = (('{}, ' * (len(event))).format(*event)).rstrip(', ') + "\n"
                    handle.write(_ev)


if __name__ == "__main__":
    oP = outputParser()
    oP.input_file("0.events.xml")
    # oP.events_of_interest = ["actend", "actstart"]
    oP.parse_matsim_output()
    oP.write_to_group("event_output/{}.txt")
