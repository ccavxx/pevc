# scraping with multiprocessing

# import packages
import pandas as pd
import numpy as np
import os
import requests
import time
import random
from bs4 import BeautifulSoup
import pandas as pd
import base64
import re
from io import BytesIO
from fontTools.ttLib import TTFont
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from PIL import Image
import cv2 as cv
import multiprocessing as mp

class Decoder: # anti-font-face coding
    def __init__(self, base64_srt):
        font = TTFont(BytesIO(self.make_font_file(base64_srt)))
        # get the decode table
        self.c = font['cmap'].tables[0].ttFont.tables['cmap'].tables[0].cmap

    def make_font_file(self, base64_string: str):
        bin_data = base64.decodebytes(base64_string.encode())
        return bin_data # return binary data

    def decode(self, string):
        ret_str = ''
        for char in string:
            code = ord(char)
            if code in self.c.keys():
                decode = self.c[code]
                decode = int(decode[-2:]) - 1
            else:
                decode = char
            ret_str += str(decode)
        return ret_str

class CaptchaSolver():
    """
    to solve slider CAPTCHA
    """
    def __init__(self, webdriver, solver_id):
        self.browser = webdriver.browser
        self.wait = webdriver.wait
        self.print_log_msg = webdriver.print_log_msg
        self.solver_id = solver_id

    def get_geetest_image_position(self):
        img = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.geetest_canvas_bg.geetest_absolute')))
        location = img.location
        size = img.size
        top, bottom, left, right = location['y'], location['y']+size['height'], \
                                    location['x'], location['x']+size['width']
        return (top, bottom, left, right)

    def get_geetest_image(self):
        top, bottom, left, right = self.get_geetest_image_position()
        screenshot = self.browser.get_screenshot_as_png()
        screenshot = Image.open(BytesIO(screenshot))
        screen_width = screenshot.size[0]
        window_width = self.browser.get_window_size()['width']
        scale = screen_width/window_width # monitor screen issue
        top, bottom, left, right = (item*scale for item in (top, bottom, left, right))

        captcha_img = screenshot.crop((left, top, right, bottom)) # note the arguments order
        return captcha_img

    def get_target_area_img(self):
        geetest_image = self.get_geetest_image()
        geetest_image.save(f'scripts/captcha/whole_area_{self.solver_id}.png')
        width, height = geetest_image.size
        target_area = geetest_image.crop((0.25*width, 0, width, height))
        target_area.save(f'scripts/captcha/target_area_{self.solver_id}.png')

    def get_track_length(self):
        length = 0
        num_fail = 0
        while length == 0:
            self.get_target_area_img()
            image = cv.imread(f'scripts/captcha/target_area_{self.solver_id}.png')
            blurred = cv.GaussianBlur(image, (5, 5), 1)
            canny = cv.Canny(blurred, 200, 400)
            contours, hierarchy = cv.findContours(canny, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)
            for i, contour in enumerate(contours):
                x, y, w, h = cv.boundingRect(contour)
                if all((75<=w<=105, 75<=h<=105)): # trained size data
                    self.print_log_msg('captcha solved')
                    cv.rectangle(image, (x, y), (x+w, y+h), (0, 0, 255), 1)
#                     cv.imwrite('scripts/captcha/fitted.png', image)
                    offset = self.get_geetest_image().size[0]*0.25/2
                    length = x + w/2 + offset + 13 # manual adjustment after trials & errors
                    return length

            # else, failed to fit
            num_fail += 1
            self.print_log_msg("failed to solve the captcha, refresh and try again")
            os.remove(f'scripts/captcha/target_area_{self.solver_id}.png')
            # save the failed img
            # for i, contour in enumerate(contours):
            #     x, y, w, h = cv.boundingRect(contour)
            #     cv.rectangle(canny, (x, y), (x+w, y+h), (0, 0, 255), 1)
            # now = time.strftime("%m%d_%H%M%S", time.localtime())
            # os.rename('scripts/captcha/whole_area.png', f'scripts/captcha/{now}_failed_whole.png')
            # os.rename('scripts/captcha/target_area.png', f'scripts/captcha/{now}_failed_target.png')
            # cv.imwrite(f'scripts/captcha/{now}_failed_canny.png', canny)

            # refresh
            time.sleep(1)
            if num_fail <= 3: # refresh captcha
                self.browser.find_element_by_css_selector('.geetest_refresh_1').click()
                time.sleep(1) # wait to load a new captcha
            else: # refresh webpage
                self.browser.refresh()
                time.sleep(5)

    def get_step_length_list(self):
        total_track_length = self.get_track_length()
        steps_number = 7
        tmp = 3**np.array(range(1, steps_number+1))
        step_length_list = tmp/np.sum(tmp) * total_track_length
        step_length_list = list(step_length_list[::-1])
        return step_length_list

    def solve(self):

        if os.path.exists('captcha') == False:
            os.mkdir('captcha')
        action = ActionChains(self.browser)
        slider_button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'geetest_slider_button')))
        step_length_list = self.get_step_length_list()
        total_track_length = sum(step_length_list)
        time.sleep(2)   # wait few seconds to submit
        self.print_log_msg('submitting solution')
        action.click_and_hold(slider_button).perform()
        time.sleep(random.uniform(0.3,0.5))
        while step_length_list: # drag
            step_length = step_length_list[0]
            vertical_movement = random.uniform(0,0.2)
            action.move_by_offset(step_length/2, vertical_movement).perform()

            # reset action to avoid auto-cumulation of steps.
            # see https://github.com/SeleniumHQ/selenium/issues/5747#issuecomment-379949052
            action = ActionChains(self.browser)

            step_length_list.remove(step_length)
            t = random.uniform(0.2,0.4)
            time.sleep(t)
        action.release(slider_button).perform()
        self.print_log_msg('submitted')
        os.remove(f'scripts/captcha/whole_area_{self.solver_id}.png')
        os.remove(f'scripts/captcha/target_area_{self.solver_id}.png')
        time.sleep(5)
        if 'clear' in self.browser.current_url:
            self.print_log_msg('accepted')
            return
        else:
            self.print_log_msg('rejected, refresh and try again')
            try: # next captcha available or not
                self.browser.find_elements_by_css_selector('.geetest_refresh_1')
            except ElementNotInteractableException: # not available. refresh the page
                self.browser.refresh()
            else: # available, load the next captcha
                self.browser.find_element_by_css_selector('.geetest_refresh_1').click()
            finally: # wait to refresh/reload
                time.sleep(5)
                self.solve()

class Scraper():

    def __init__(self, show_window=True, show_log_msg=True):
        chrome_options = Options()
        chrome_options.add_argument('window-size=1000,800')
        if show_window == False:
            chrome_options.add_argument("--headless")
        self.browser = webdriver.Chrome(options=chrome_options)
        self.show_log_msg = show_log_msg
        self.wait = WebDriverWait(self.browser, 15)

    def visit(self, url):
        self.browser.get(url)

    def print_log_msg(self, msg):
        if self.show_log_msg:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"{now}: " + msg)

    def get_event_detail(self, event, decoder):
        """
        Extract the detail of a single event.
        including: investee's info, investors' info, etc
        event: an selenium element, corresponds to a row in the page table
        """
        investee_tag1 = event.find_element_by_css_selector('.tp1 [href]')
        investee_shortname = investee_tag1.get_attribute('title')
        investee_url = investee_tag1.get_attribute('href')
        investee_id = re.sub("\D", "", investee_url)

        investee_tag2 = event.find_element_by_css_selector('.tp2_com')
        investee_fullname = investee_tag2.text

        investor_tags = event.find_elements_by_tag_name('td')[4]
        investor_tags = investor_tags.find_elements_by_tag_name('a')
        investor_number = len(investor_tags)
        if investor_number == 0:
            investor_names = '未透露'
            investor_urls =  'unknown'
            investor_ids = investor_leader_id = '9999999'
        else:
            investor_names = [investor.get_attribute('title') for investor in investor_tags]
            investor_names = decoder.decode(",".join(investor_names))
            investor_urls = ",".join([investor.get_attribute('href') for investor in investor_tags])
            investor_ids = ",".join([re.sub("\D", "", investor.get_attribute('href')) for investor in investor_tags])
            investor_leader_id = investor_ids[:7]

        industries_tag = event.find_element_by_css_selector('.tp3')
        industries = decoder.decode(industries_tag.text.replace('\n', ','))
        # industries = 'unknown' if industries == "未透露" else industries

        amount = event.find_elements_by_tag_name('td')[2].text.strip()
        amount = decoder.decode(amount)
        # amount = 'unknown' if amount == "未披露" else amount

        series = event.find_elements_by_tag_name('td')[3].text
        # series = 'unknown' if amount == "未披露" else amount

        date_tag = event.find_elements_by_tag_name('td')[6]
        date = decoder.decode(date_tag.text)

        event_id = date.replace("-","") + investee_id + investor_leader_id
        event_detail = [event_id,
                        investee_id, investee_shortname, investee_fullname, investee_url,
                        investor_number, investor_ids, investor_leader_id, investor_names, investor_urls,
                        series, amount, date, industries
                       ]
        return event_detail

#        # one can also use beautifulsoup
#         soup = BeautifulSoup(scraper.browser.page_source, 'html.parser')
#         events = soup.find_all('tbody')[0].find_all('tr')[1:]
#         event = events[0].find_all('td')
#         investee_shortname = event[0].a['title']
#         investee_fullname = event[1].find_all('span')[1].text

    def get_current_page_table(self):
        """
        Get the table of the current page.
        """
        events_list = self.browser.find_elements_by_class_name('table-plate3')
        page_table = []
        base64_str = re.search("base64,(.*?)'\)", self.browser.page_source)
        if base64_str == None:
            self.print_log_msg(f'decoder key not found, refresh and try again')
            return page_table # empty
        base64_str = base64_str.group(1)
        decoder = Decoder(base64_str)
        for event in events_list:
            event_detail = self.get_event_detail(event, decoder)
            page_table.append(event_detail)
        # convert to data frame
        page_table = pd.DataFrame(data=page_table,
                                  columns=['event_id',
                                            'investee_id', 'investee_shortname', 'investee_fullname', 'investee_url',
                                            'investor_number', 'investor_ids', 'investor_leader_id', 'investor_names', 'investor_urls',
                                            'series', 'amount', 'date', 'industries'])
        self.print_log_msg(f'scraped {len(page_table)} events')
        return page_table

    def load_and_scrape(self, url, solver_id):
        """
        solver_id is used to distinguish images saved to local directories
        created by different captcha solvers when multiprocessing is enabled.
        """
        self.print_log_msg(f'loading')
        self.visit(url)
        time.sleep(1) # wait to load
        if self.browser.find_elements_by_class_name('table-plate3'): # table found
            self.print_log_msg(f'table detected')
        else:
            self.print_log_msg("still loading, may require captcha")
            try: # whether captcha is asked
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.geetest_canvas_bg.geetest_absolute')))
            except:
                self.print_log_msg("no captcha, just scrape")
            else:
                self.print_log_msg(f"captcha required, try to solve it. solver_id = {solver_id}")
                captcha_solver = CaptchaSolver(self, solver_id)
                captcha_solver.solve()
            finally:
                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'table-plate3')))
                self.print_log_msg(f'table detected')
        self.print_log_msg('scraping')
        page_table = self.get_current_page_table()
        return page_table

    def get_events_in_a_year(self, year, start_page=1, end_page=None, cache_table=False):
        """
        Get the tables from the start page to the end page
        """
        if os.path.exists('data') == False:
            os.mkdir('data')
        years_pages = {2019:483, 2018:861, 2017:1016, 2016:1281, 2015:892, \
                        2014:434, 2013:155, 2012:135, 2011:131, 2010:176}
        if end_page == None: # scrape all
            end_page = year_pages[year]

        self.print_log_msg(f'start to scrape from page {start_page} to {end_page} in {year}')
        events_table = pd.DataFrame()
        pages_to_scrape = range(start_page, end_page+1)
        for page in tqdm(pages_to_scrape):
            url = f"https://data.cyzone.cn/event/list-0-1-0-{year}0101-{year}1231-0-{page}/0"
            page_table = pd.DataFrame()
            num_trial = 0
            while True:
                num_trial += 1
                try:
                    self.print_log_msg(f"going to page {page}")
                    page_table = self.load_and_scrape(url, f'{year}{str(page).zfill(4)}')
                except Exception as e:
                    self.print_log_msg('get an error:')
                    now = time.strftime("%Y%m%d_%H%M%S", time.localtime())
                    self.browser.get_screenshot_as_file(f"capthca/error_screenshot_at_{now}.png")
                    print(e)
                    time.sleep(10) # wait a moment and try again
                if len(page_table)>0:
                    break
                if num_trial == 10:
                    break
                    self.print_log_msg(f"unexpected error on page {page}")
            page_table['page_number'] = page # used to check if all pages are scraped
            events_table = pd.concat([events_table, page_table])
            if cache_table:
                events_table.to_csv(f'data/events_{year}_pg{start_page}to{page}.csv', index=False)
                if(os.path.exists(f"data/events_{year}_pg{start_page}to{page-1}.csv")):
                    os.remove(f"data/events_{year}_pg{start_page}to{page-1}.csv")

        self.print_log_msg(f'finished scraping from page {start_page} to {end_page} in {year}' )

        return events_table

class MP_Scraper():
    def __init__(self, show_log_msg=True):
        self.show_log_msg = show_log_msg

    def print_log_msg(self, msg):
        if self.show_log_msg:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"{now}: " + msg)

    def subprocess_scrape(self, year, start_page, end_page):
        start_time = time.time()
        self.print_log_msg(f'starting a subprocess scraping events in {year} from page {start_page} to {end_page}')
        scraper = Scraper(show_window=False, show_log_msg=False)
        events_tb = scraper.get_events_in_a_year(year=year, start_page=start_page, end_page=end_page)
        end_time = time.time()
        self.print_log_msg(f'scraped events in {year} from page {start_page} to {end_page} in {round(end_time-start_time,0)} seconds')
        events_tb.to_csv(f'data/events_{year}_pg{start_page}to{end_page}.csv', index=False)

    def concat_files(self, dir_name, input_prefix, output_filename, delete=False):
        tbs = pd.DataFrame()
        count = 0
        for file_name in os.listdir(dir_name):
            if input_prefix in file_name:
                file_path = dir_name+file_name
                tb = pd.read_csv(file_path)
                tbs = pd.concat([tbs, tb])
                if delete:
                    os.remove(file_path)
                count += 1
        tbs = tbs.sort_values(['date'], ascending=False)
        tbs.to_csv(f'{dir_name}{output_filename}', index=False)
        self.print_log_msg(f"concatenated {count} files in {dir_name} with prefix '{input_prefix}' and save to {output_filename}")

    def multiprocess_scrape(self, start_year, end_year, num_cores_to_use):
        self.print_log_msg(f'start scraping events from year {start_year} to {end_year}')
        years = list(range(start_year, end_year+1))
        years_pages = {2019:483, 2018:861, 2017:1016, 2016:1281, 2015:892, \
                        2014:434, 2013:155, 2012:135, 2011:131, 2010:176}
        for year in years:
            total_pages = years_pages[year]
            pages_per_core = np.ceil(total_pages/num_cores_to_use)
            start_pages = 1+np.array(range(num_cores_to_use))*pages_per_core
            end_pages = start_pages + pages_per_core - 1 # remember to minus 1
            end_pages[-1] = total_pages

            self.print_log_msg(f'start scraping events in {year}, in total {total_pages} pages')
            self.print_log_msg(f'initializing {num_cores_to_use} subprocesses...')
            pool = mp.Pool(num_cores_to_use)
            for start_page, end_page in zip(start_pages, end_pages):
                pool.apply_async(self.subprocess_scrape, args=(year, int(start_page), int(end_page)))
            pool.close()
            pool.join()
            self.print_log_msg(f'finished scraping all events in year {year}')

            # concatenate tables of pages in a year from subprocesses
            self.concat_files('data/', f'events_{year}_pg', f'events_{year}.csv', delete=True)

        # concatenate tables of pages in years
        if len(years)>1:
            self.concat_files('data/', 'events_20', f'events_fr{start_year}to{end_year}.csv', delete=False)

if __name__== '__main__':
    start_year = 2010
    end_year = 2019
    num_cores_to_use = os.cpu_count()
    mp_scraper = MP_Scraper()
    mp_scraper.multiprocess_scrape(start_year, end_year, num_cores_to_use)
