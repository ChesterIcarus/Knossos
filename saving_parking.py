import os
import time
import requests
from lxml import html
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.request import urlopen
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC


parking_lots = dict()

# top_x = 33.420826000000
# top_y = -111.933035
# bottom_x = 32.409927
# bottom_y = -112.913627
movement = .006
top_x = 33.479525
top_y =  -112.108821
bottom_x = 33.387411
bottom_y= -111.894073

browser = webdriver.Firefox()
browser.set_window_size(5000, 5000)
time.sleep(1)
url = "https://www.parkme.com/map#Phoenix"
while top_x > bottom_x:
    top_y =  -112.108821
    while top_y < bottom_y:
        browser.execute_script('''window.open("http://google.com","_blank");''')
        ui.WebDriverWait(browser, 2).until(EC.new_window_is_opened)
        browser.switch_to_window(browser.window_handles[1])
        req_url = ("https://www.parkme.com/map#{}%2C{}").format(str(top_x), str(top_y))
        browser.get(req_url)
        # locator='/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[2]/div[2]/div[1]'
        locator="//*[@id=\"loading\"]"
        timeout=6
        try:
            ui.WebDriverWait(browser, timeout).until(EC.invisibility_of_element_located((By.XPATH, locator)))
        except Exception as ex:
            try:
                new_loc = "/html/body/div[2]/div[5]/div[2]/div/div"
                ui.WebDriverWait(browser, .25).until(EC.presence_of_element_located((By.XPATH, new_loc)))
                browser.switch_to_window(browser.window_handles[0])
            except Exception as ex:
                top_y -= movement
                continue

        soup = BeautifulSoup(browser.page_source, 'html.parser')
        lot_divs = soup.find_all("div", {"class":"featured_lot_container"})
        for lot in lot_divs:
            name = lot.find("div", {'class':"fle_lot_name"})
            address = lot.find("div",{'class':"fle_lot_address"})
            parking_lots[name.text] = {'name':name.text, 'amen':[], 'cost':None, "address":address.text}
            amen = lot.find_all("img", {'class':"amenity-asset-city"})
            for amens in amen:
                parking_lots[name.text]['amen'].append(str(amens.title))
            cost = lot.find("a", {'class':"left btn btn-primary btn-small fle_reserve compare-res-btn"})
            cost = cost.text.replace("$", "")
            parking_lots[name.text]['cost'] = cost
        # print(browser.current_url)
        browser.delete_all_cookies()
        browser.close()
        browser.switch_to_window(browser.window_handles[0])
        print(len(parking_lots.keys()))
        top_y += movement
        # browser.quit()
    top_x -= movement
browser.quit()

for key in parking_lots:
    print(key)