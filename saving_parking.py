from stem import Signal
from stem.control import Controller
import os

with Controller.from_port(port = 9051) as controller:
    controller.authenticate()
    print("Success!")
    controller.signal(Signal.NEWNYM)
    print("New Tor connection processed")
    print(controller.get_latest_heartbeat)
    os.system("dig +short myip.opendns.com @resolver1.opendns.com")