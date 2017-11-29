import os
import time
import json
import requests
from lxml import html
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.request import urlopen
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
        

class parking_data_ret(object):

    def __init__(self, top_x, top_y, bottom_x, bottom_y, step_val, browser_x, browser_y, browser_timeout):
        print("Starting data retrieval")
        print("Top X: "+str(top_x)+", Top y: "+str(top_y)+", Bottom X: "+str(bottom_x)+", Bottom y: "+str(bottom_y))
        print("Step Value: "+str(step_val))
        print("Browser dimension is: "+str(browser_x)+" x "+str(browser_y))
        print("Browser timeout is: "+str(browser_timeout))
        self.top_x = top_x
        self.top_y = top_y
        self.bottom_x = bottom_x
        self.bottom_y = bottom_y
        self.step_val = step_val
        self.browser_x = browser_x
        self.browser_y = browser_y
        self.browser_timeout = browser_timeout
        self.parking_lots = dict()

    
    def main_looping(self, depth, output_file):
        os.environ['MOZ_HEADLESS'] = '1'
        self.output_file = output_file
        browser = webdriver.Firefox()
        browser.set_window_size(self.browser_x, self.browser_y)
        time.sleep(1)
        try:
            while self.top_x > self.bottom_x:
                self.top_y =  -112.108821
                while self.top_y < self.bottom_y:
                    browser.execute_script('''window.open("http://google.com","_blank");''')
                    # Fixed buffer of two second for "new tab" spawing
                    ui.WebDriverWait(browser, 2).until(EC.new_window_is_opened)
                    browser.switch_to_window(browser.window_handles[1])
                    req_url = ("https://www.parkme.com/map#{}%2C{}").format(str(self.top_x), str(self.top_y))
                    browser.get(req_url)
                    # locator='/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[2]/div[2]/div[1]'
                    loading_locator = "//*[@id=\"loading\"]"
                    # locator="/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[2]/div[3]/img[1]"
                    locator = "//*[@id=\"results\"]/div/div"
                    try:
                        ui.WebDriverWait(browser, self.browser_timeout).until(EC.invisibility_of_element_located((By.XPATH,loading_locator)))
                        ui.WebDriverWait(browser, 2).until(EC.visibility_of_element_located((By.XPATH, locator)))
                    except Exception as ex:
                        try:
                            new_loc = "/html/body/div[2]/div[5]/div[2]/div/div"
                            ui.WebDriverWait(browser, .25).until(EC.presence_of_element_located((By.XPATH, new_loc)))
                            browser.switch_to_window(browser.window_handles[0])
                        except Exception as ex:
                            self.top_y -= self.step_val
                            continue
                    soup = BeautifulSoup(browser.page_source, 'lxml')
                    lot_divs = soup.find_all("div", {"class":"featured_lot_container"})
                    for lot in lot_divs:
                        name = lot.find("div", {'class':"fle_lot_name"}).text
                        address = lot.find("div",{'class':"fle_lot_address"}).text
                        if name not in self.parking_lots:
                            if depth == 'shallow':
                                self.shallow_scraping(browser, lot, name, address)
                            if depth == 'deep':
                                self.deep_scraping(browser, lot, name, address)
                    # print(browser.current_url)
                    browser.delete_all_cookies()
                    browser.close()
                    browser.switch_to_window(browser.window_handles[0])
                    print(len(self.parking_lots.keys()))
                    self.top_y += self.step_val
                    # browser.quit()
                self.top_x -= self.step_val
            browser.quit()
        except Exception as unknown:
            print(unknown)
        except KeyboardInterrupt as key:
            print("Interrupted")
        finally:
            browser.quit()
            f_ = open(output_file, 'w')
            json.dump(self.parking_lots, f_, indent=4)
            f_.close()


    def shallow_scraping(self, browser, lot, name, address):
        try:
            self.parking_lots[name] = {'name':name, 'amen':[], 'cost':'', "address":address}
            amen = lot.find_all("img", {'class':"amenity-asset-city"})
            for amens in amen:
                self.parking_lots[name.text]['amen'].append(amens['data-tooltip'])
            cost = lot.find("a", {'class':"left btn btn-primary btn-small fle_reserve compare-res-btn"}).string
            self.parking_lots[name.text]['cost'] = cost
        except KeyboardInterrupt as key:
            print("Interrupted")
            browser.quit()
            f_ = open(self.output_file, 'w')
            json.dump(self.parking_lots, f_, indent=4)
            f_.close()


    def deep_scraping(self, browser, lot, name, address):
        try:
            self.parking_lots[name] = {'name':name, 'amen':[], 'cost':{}, "address":address}
            # meta_xpath = "/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[3]/div[1]/a"
            meta_xpath = "/html/body/div[4]/form/div/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/h1"
            lot_url = ("https://www.parkme.com{}").format(lot.find("a", {"class":'left btn btn-primary btn-small fle_reserve compare-res-btn'})['href'])
            browser.execute_script('''window.open("http://google.com","_blank");''')
            ui.WebDriverWait(browser, 2).until(EC.new_window_is_opened)
            browser.switch_to_window(browser.window_handles[-1])
            browser.get(lot_url)
            try:
                ui.WebDriverWait(browser, self.browser_timeout).until(EC.visibility_of_element_located((By.XPATH, meta_xpath)))
            except Exception as noFound:
                pass
            soup = BeautifulSoup(browser.page_source, 'lxml')

            # Lot Rate Cost
            rate_rows = soup.find_all('div', {"class": 'module-table-row module-no-border'})
            for row in rate_rows:
                try:
                    rate_type = row.find('div', {'class':"left lot-rate-type"}).text
                    rate = row.find('div', {'class':"left lot-rate-type"}).text
                    self.parking_lots[name]['cost'][rate_type] = rate
                except Exception as notHere:
                    continue

            # Lot Payment Accepted
            try:
                payment_array = (soup.find('div', {'itemprop':"paymentAccepted"}).text).split(',')
                self.parking_lots[name]['paymentAccepted'] = payment_array
            except Exception as ex:
                self.parking_lots[name]['paymentAccepted'] = "No Data"
                pass

            # Hours of operation
            hour_rows = soup.find_all("div", {'class':'module-table-row no-border-row'})
            self.parking_lots[name]['hours'] = list()
            for row in hour_rows:
                try:
                    operation = row.find("div", {'class':"left"}).text
                    try:
                        hrs = row.find("div", {'class':"right"}).text
                        self.parking_lots[name]['hours'].append({operation:hrs})
                    except Exception as noHours:
                        continue
                except Exception as full:
                    continue
            
            # General metadata about the lot
            meta = soup.find("table", {"class":"module-table-group operator-table"})
            self.parking_lots[name]['meta'] = dict()
            meta_rows = meta.findChildren('tr')
            for row in meta_rows:
                try:
                    meta_key = row.find('td',{'class':'table-cell-header'}).text
                    meta_value = row.find('td', {'class':"table-cell-value"})
                    try:
                        self.parking_lots[name]['meta'][meta_key] = meta_value.text
                    except Exception as ex:
                        pass
                except Exception as ex:
                    continue
            
            # Amenities for given lot
            amens = soup.find_all('div', {'class':'amenity-wrapper'})
            for amen in amens:
                try:
                    this_amen = amen.find('img', {'class':'amenity-asset-city'})
                    self.parking_lots[name]['amen'].append(str(this_amen['title']))
                except Exception as ex:
                    continue
            browser.close()
            browser.switch_to_window(browser.window_handles[0])
        except KeyboardInterrupt as key:
            print("Interrupted")
            browser.quit()
            f_ = open(self.output_file, 'w')
            json.dump(self.parking_lots, f_, indent=4)
            f_.close()
#results > div > div
if __name__ == "__main__":
    example = parking_data_ret(33.479525,-112.108821, 33.387411, -111.894073, .01, 10000, 10000, 15)
    example.main_looping('deep','./Parking_Data/Minimal_metadata.txt')


# movement = .01
# top_x = 33.479525
# top_y =  -112.108821
# bottom_x = 33.387411
# bottom_y= -111.894073
# os.environ['MOZ_HEADLESS'] = '1'
# browser = webdriver.Firefox()
# browser.set_window_size(10000, 10000)
# time.sleep(1)
