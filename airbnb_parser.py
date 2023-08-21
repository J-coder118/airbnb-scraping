import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import pandas as pd
from multiprocessing import Pool

import json
import time

import os

RULES_SEARCH_PAGE = {
    'url': {'tag': 'a', 'get': 'href'},
    'title': {'tag': 'div', 'class': 't1jojoys'},
    'rating_n_reviews': {'tag': 'span', 'class': 'r1dxllyb'},
    'img_cover': {'tag': 'img', 'get': 'data-original-uri'},
    'price': {'tag': 'span', 'class': '_tyxjp1'},
    'ADR': {'tag': 'span', 'class': '_tyxjp1'},
}

RULES_DETAIL_PAGE = {
    'roomtype_type': {'tag': 'h2', 'class': 'hpipapi'},
    'bedrooms': {'tag': 'li', 'class': 'l7n4lsf', 'order': 1},
    'bathrooms': {'tag': 'li', 'class': 'l7n4lsf', 'order': 3},
    'property_type': {'tag': 'div', 'class': 'i1303y2k'},
    'amenities_available': {'tag': 'button', 'class': 'l1ovpqvx'}, 
    'real_estate_type': {'tag': 'div', 'class': 'i1303y2k'},
    'accommodates': {'tag': 'li', 'class': 'l7n4lsf', 'order': 0},
}

def extract_listings(page_url):
    """Extracts all listings from a given page"""
    listings_max = 0
    listings_out = [BeautifulSoup('', features='html.parser')]

    try:
        answer = requests.get(page_url, timeout=5)
        content = answer.content
        soup = BeautifulSoup(content, features='html.parser')
        listings = soup.findAll("div", {"class": "cy5jw6o"})
    except:
        # if no response - return a list with an empty soup
        listings = [BeautifulSoup('', features='html.parser')]
    if len(listings) == 20:
        listings_out = listings
    elif len(listings) >= listings_max:
        listings_max = len(listings)
        listings_out = listings
    
    with open("example.txt", "w" , encoding="utf-8") as file:
        file.write(str(soup))
    
    # print("listing", soup)
    return listings_out
              
def extract_element_data(soup, params):
    """Extracts data from a specified HTML element"""
    # 1. Find the right tag
    if 'class' in params:
        elements_found = soup.find_all(params['tag'], params['class'])
    else:
        elements_found = soup.find_all(params['tag'])
    # 2. Extract text from these tags
    if 'get' in params:
        element_texts = [el.get(params['get']) for el in elements_found]
    else:
        element_texts = [el.get_text() for el in elements_found]
    # 3. Select a particular text or concatenate all of them
    tag_order = params.get('order', 0)
    if tag_order == -1:
        output = '**__**'.join(element_texts)
    else:
        output = element_texts[tag_order]

    return output

def extract_listing_features(soup, rules):# individual listing
    """Extracts all features from the listing"""
    features_dict = {}
    for feature in rules:
        try:
            temp = extract_element_data(soup, rules[feature])
            if feature == "bedrooms":
                clean_bed = temp.split(" · ")
                temp = clean_bed[1]
            if feature == "bathrooms":
                clean_bath = temp.split(" · ")
                temp = clean_bath[1]
            features_dict[feature] = temp
        except:
            features_dict[feature] = 'empty'

    return features_dict

def extract_soup_js(listing_url, waiting_time=[20, 1]):
    """Extracts HTML from JS pages: open, wait, click, wait, extract"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--blink-settings=imagesEnabled=false')
    driver = webdriver.Chrome(options=options)
    driver.get(listing_url)
    time.sleep(waiting_time[0])
    time.sleep(waiting_time[1])
    detail_page = driver.page_source
    driver.quit()
    # answer = requests.get(listing_url, timeout=5)
    # detail_page = answer.content

    return BeautifulSoup(detail_page, features='html.parser')

def scrape_detail_page(base_features):
    """Scrapes the detail page and merges the result with basic features"""
    detailed_url = 'https://www.airbnb.com' + base_features['url']

    soup_detail = extract_soup_js(detailed_url)
    features_detailed = extract_listing_features(soup_detail, RULES_DETAIL_PAGE)
    features_all = {**base_features, **features_detailed}

    return features_all

def extract_amenities(soup):
    amenities = soup.find_all('div', {'class': '_aujnou'})
    amenities_dict = {}

    for amenity in amenities:
        header = amenity.find('div', {'class': '_1crk6cd'}).get_text()
        values = amenity.find_all('div', {'class': '_1dotkqq'})
        values = [v.find(text=True) for v in values]
        amenities_dict['amenity_' + header] = values
        
    return json.dumps(amenities_dict)


class Parser:
    def __init__(self, link, out_file):
        self.link = link
        self.out_file = out_file
        self.count = 0
    
    def build_urls(self, listings_per_page=18, pages_per_location=15):
        """Builds links for all search pages for a given location"""
        url_list = []

        for i in range(500, 505, 5):
            minPrice = i
            maxPrice = i + 5
            url = self.link + f'price_min={minPrice}&price_max={maxPrice}'
            for i in range(pages_per_location):
                offset = listings_per_page * i
                url_pagination = url + f'&items_offset={offset}'
                url_list.append(url_pagination)
                self.url_list = url_list
        print("-----------complete building urls------------")
            
    def process_search_pages(self):
        """Extract features from all search pages"""
        features_list = []

        for page in self.url_list:
            listings = extract_listings(page)
            self.count = self.count + len(listings)
            for listing in listings:
                features = extract_listing_features(listing, RULES_SEARCH_PAGE)
                features['sp_url'] = page
                features_list.append(features)
        self.base_features_list = features_list

        print("------------complete process search pages---------------")

    def process_detail_pages(self):
        """Runs detail pages processing in parallel"""
        n_pools = os.cpu_count() // 2

        with Pool(n_pools) as pool:
            result = pool.map(scrape_detail_page, [feature for feature in self.base_features_list])
        pool.close()
        pool.join()
        self.all_features_list = result

        print("--------------complete process detail pages---------------")

    def save(self, feature_set='all'):
        if feature_set == 'basic':
            pd.DataFrame(self.base_features_list).to_csv(self.out_file, index=False)
        elif feature_set == 'all':
            pd.DataFrame(self.all_features_list).to_csv(self.out_file, index=False)
        else:
            pass
        
    def parse(self):
        t0 = time.time()
        self.build_urls()
        self.process_search_pages()
        self.process_detail_pages()
        self.save('all')
        print("Total Time",time.time() - t0, "count", self.count)
