import os
import time
import json
import random
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
        print("self.browser dimension is: "+str(browser_x)+" x "+str(browser_y))
        print("self.browser timeout is: "+str(browser_timeout))
        self.top_x = top_x
        self.top_y = top_y
        self.bottom_x = bottom_x
        self.bottom_y = bottom_y
        self.step_val = step_val
        self.browser_x = browser_x
        self.browser_y = browser_y
        self.browser_timeout = browser_timeout
        self.parking_lots = dict()
        self.count = 25
        self.USER_AGENT_CHOICES = [
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:23.0) Gecko/20100101 Firefox/23.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:24.0) Gecko/20140205 Firefox/24.0 Iceweasel/24.3.0',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
        ]

    
    def main_looping(self, depth, output_file):
        os.environ['MOZ_HEADLESS'] = '1'
        self.output_file = output_file
        profile = webdriver.FirefoxProfile()
        # profile.set_preference("general.useragent.override", "New user agent")
        # profile.set_preference('network.proxy.type', 1)
        # profile.set_preference('network.proxy.http', 'localhost')
        # profile.set_preference('network.proxy.http_port', '9000')
        # THIS MUST BE DONE BEHIND A PROXY
        # They ban IP's for crawling
        # self.browser = webdriver.Firefox(firefox_profile=profile)
        self.browser = webdriver.Firefox()
        self.browser.set_window_size(self.browser_x, self.browser_y)
        time.sleep(1)
        try:
            while self.top_x > self.bottom_x:
                self.top_y =  -112.108821
                while self.top_y < self.bottom_y:
                    self.browser.execute_script('''window.open("http://google.com","_blank");''')
                    # Fixed buffer of two second for "new tab" spawing
                    ui.WebDriverWait(self.browser, 2).until(EC.new_window_is_opened)
                    self.browser.switch_to_window(self.browser.window_handles[1])
                    req_url = ("https://www.parkme.com/map#{}%2C{}").format(str(self.top_x), str(self.top_y))
                    self.browser.get(req_url)
                    # locator='/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[2]/div[2]/div[1]'
                    loading_locator = "//*[@id=\"loading\"]"
                    locator="/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[2]/div[3]/img[1]"
                    # locator = "//*[@id=\"results\"]/div/div"
                    try:
                        ui.WebDriverWait(self.browser, self.browser_timeout).until(EC.invisibility_of_element_located((By.XPATH,loading_locator)))
                        ui.WebDriverWait(self.browser, self.browser_timeout).until(EC.visibility_of_element_located((By.XPATH, locator)))
                    except Exception as ex:
                        try:
                            new_loc = "/html/body/div[2]/div[5]/div[2]/div/div"
                            ui.WebDriverWait(self.browser, .25).until(EC.presence_of_element_located((By.XPATH, new_loc)))
                            self.browser.switch_to_window(self.browser.window_handles[0])
                        except Exception as ex:
                            self.top_y -= self.step_val
                            continue
                    soup = BeautifulSoup(self.browser.page_source, 'lxml')
                    lot_divs = soup.find_all("div", {"class":"featured_lot_container"})
                    for lot in lot_divs:
                        name = lot.find("div", {'class':"fle_lot_name"}).text
                        address = lot.find("div",{'class':"fle_lot_address"}).text
                        if name not in self.parking_lots:
                            if depth == 'shallow':
                                self.shallow_scraping(lot, name, address)
                            if depth == 'deep':
                                self.deep_scraping(lot, name, address)
                    # print(self.browser.current_url)
                    self.browser.delete_all_cookies()
                    self.browser.close()
                    self.browser.switch_to_window(self.browser.window_handles[0])
                    print(len(self.parking_lots.keys()))
                    self.top_y += self.step_val
                    # self.browser.quit()
                self.top_x -= self.step_val
            self.browser.quit()
        except Exception as unknown:
            print(unknown)
        except KeyboardInterrupt as key:
            print("Interrupted")
        finally:
            self.output()


    def shallow_scraping(self, lot, name, address):
        try:
            self.parking_lots[name] = {'name':name, 'amen':[], 'cost':'', "address":address}
            amen = lot.find_all("img", {'class':"amenity-asset-city"})
            for amens in amen:
                self.parking_lots[name.text]['amen'].append(amens['data-tooltip'])
            cost = lot.find("a", {'class':"left btn btn-primary btn-small fle_reserve compare-res-btn"}).string
            self.parking_lots[name.text]['cost'] = (str(cost)).strip()
        except KeyboardInterrupt as key:
            self.output()


    def deep_scraping(self, lot, name, address):
        try:
            if (self.count >= 10):
                print("Sleeping for random time.")
                time.sleep(random.random(0, 5))
                # TODO: in the Future
                # self.browser.quit()
                # profile = webdriver.FirefoxProfile()
                # profile.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'")
                # self.browser = webdriver.Firefox()
                self.count = 0
            self.parking_lots[name] = {'name':name, 'amen':[], 'cost':{}, "address":address}
            # meta_xpath = "/html/body/div[2]/div[5]/div[2]/div/div/div[1]/div/div/div[3]/div[1]/a"
            meta_xpath = "/html/body/div[4]/form/div/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/h1"
            lot_url = ("https://www.parkme.com{}").format(lot.find("a", {"class":'left btn btn-primary btn-small fle_reserve compare-res-btn'})['href'])
            ui.WebDriverWait(self.browser, 2).until(EC.new_window_is_opened)
            self.browser.switch_to_window(self.browser.window_handles[-1])
            self.browser.get(lot_url)
            try:
                ui.WebDriverWait(self.browser, self.browser_timeout).until(EC.visibility_of_element_located((By.XPATH, meta_xpath)))
            except Exception as noFound:
                pass
            soup = BeautifulSoup(self.browser.page_source, 'lxml')

            # Lot Rate Cost
            rate_rows = soup.find_all('div', {"class": 'module-table-row module-no-border'})
            for row in rate_rows:
                try:
                    rate_type = (str(row.find('div', {'class':"left lot-rate-type"}).text)).strip()
                    rate = row.find('div', {'class':"right"}).text
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
                    operation = (str(row.find("div", {'class':"left"}).text)).strip()
                    try:
                        hrs = (str(row.find("div", {'class':"right"}).text)).strip()
                        self.parking_lots[name]['hours'].append({operation:hrs})
                    except Exception as noHours:
                        continue
                except Exception as full:
                    continue
            
            # General metadata about the lot
            try:
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
            except Exception as type_err:
                self.output()
            
            # Amenities for given lot
            amens = soup.find_all('div', {'class':'amenity-wrapper'})
            for amen in amens:
                try:
                    this_amen = amen.find('img', {'class':'amenity-asset-city'})
                    self.parking_lots[name]['amen'].append(str(this_amen['title']))
                except Exception as ex:
                    continue
            self.browser.delete_all_cookies()
            self.browser.close()
            self.browser.switch_to_window(self.browser.window_handles[0])
            self.count += 1
        except KeyboardInterrupt as key:
            print("Interrupted")
            self.output()
    
    def output(self):
        if self.browser != None:
            try:
                self.browser.quit()
            except Exception as ex:
                print(ex)
        f_ = open(self.output_file, 'w')
        json.dump(self.parking_lots, f_, indent=4)
        f_.close()

        f_ = open((self.output_file[:-4] + "_lat_lng.txt"), 'w')
        f_.write("Top X: "+str(self.top_x)+", Top Y:"+str(self.top_y))
        f_.write("Bottom X: "+str(self.bottom_x)+", Bottom Y:"+str(self.bottom_y))
        f_.close()

#results > div > div
if __name__ == "__main__":
    example = parking_data_ret(33.479525,-112.108821, 33.387411, -111.894073, .008, 1000, 1000, 15)
    example.main_looping('deep','./Parking_Data/Minimal_metadata_02.txt')


# movement = .01
# top_x = 33.479525
# top_y =  -112.108821
# bottom_x = 33.387411
# bottom_y= -111.894073
# os.environ['MOZ_HEADLESS'] = '1'
# self.browser = webdriver.Firefox()
# self.browser.set_window_size(10000, 10000)
# time.sleep(1)
