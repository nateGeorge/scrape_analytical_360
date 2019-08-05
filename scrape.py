from pymongo import MongoClient
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

archive_url = 'http://archive.analytical360.com/testresults'

def clean_pct_col(x):
    """
    Converts percent column to numeric, replacing ND with NA.
    """
    x = x.replace('%', '')
    if 'ND' in x:
        x = 0
    else:
        x = float(x)

    return x


def clean_mg_col(x):
    """
    Converts percent column to numeric, replacing ND with NA.
    """
    x = x.replace('mg', '')
    if 'ND' in x:
        x = 0
    else:
        x = float(x)

    return x


class main_page_scraper(object):
    def __init__(self):
        self.ua = UserAgent()
        self.driver = self.setup_driver()
        self.main_url = 'http://analytical360.com/testresults'
        self.pages = ['Flower', 'Concentrate', 'Edible', 'Liquid', 'Topical']
        self.main_url = 'https://analytical360.com/testresults?perpage=1000&tab={}'


    def setup_driver(self):
        driver = webdriver.Firefox()
        # driver.set_window_size(1920, 1080)
        return driver


    def extract_all_tables(self):
        """
        Gets all info from current test tables (main page, not the archives).
        """
        column_names = ['link', 'name', 'thc_total', 'cbd_total', 'terpene_total', 'company', 'type']
        df = pd.DataFrame(columns=column_names)

        for page in self.pages:
            self.driver.get(self.main_url.format(page))
            product_table = self.driver.find_element_by_id('resultTable')
            table_entries = product_table.find_elements_by_tag_name('tr')

            # first two rows seem to be header and footer column labels
            for row in table_entries[2:]:
                data = self.extract_table_info(row)
                data['type'] = page.lower()
                add_df = pd.DataFrame(data=data, index=[0])
                df = df.append(add_df)

        df.reset_index()
        return df


    def extract_table_info(self, row):
        """
        Retrieves all information from a table row from test summary page.
        """
        columns = row.find_elements_by_tag_name('td')
        link_element = columns[0].find_element_by_tag_name('a')
        link = link_element.get_attribute('href')
        name = link_element.text
        thc_total = columns[1].text
        cbd_total = columns[2].text
        terpene_total = columns[3].text
        company = columns[4].text
        data = {'link': link,
                'name': name,
                'thc_total': thc_total,
                'cbd_total': cbd_total,
                'terpene_total': terpene_total,
                'company': company}

        return data


    def get_mg_pct_dfs(self, df):
        """
        Takes dataframe from summary tables and breaks into milligram and percent
        dataframes, and cleans them.  Returns two cleaned dfs.
        """
        # break up into those with percents and those with mg
        mg_df = df[df['thc_total'].str.contains('mg')].copy()
        pct_df = df[df['thc_total'].str.contains('%')].copy()

        pct_and_mg_columns = ['thc_total', 'cbd_total', 'terpene_total']


        for col in pct_and_mg_columns:
            pct_df[col] = pct_df[col].apply(clean_pct_col)
            mg_df[col] = mg_df[col].apply(clean_mg_col)

        return pct_df, mg_df


    def store_mg_pct_dfs(self):
        """
        Stores mg and pct dataframes into mongodb, and checks for duplicates.
        """
        conn = MongoClient()
        db = conn['analytical360']
        current_mg_data = pd.DataFrame(list(db['summary_tables_mg'].find({}, {'_id': False})))
        current_pct_data = pd.DataFrame(list(db['summary_tables_pct'].find({}, {'_id': False})))

        df = self.extract_all_tables()
        pct_df, mg_df = self.get_mg_pct_dfs(df)
        pct_df = pct_df.append(current_pct_data, sort=False)
        mg_df = mg_df.append(current_mg_data, sort=False)
        pct_df.drop_duplicates(inplace=True, keep=False)
        mg_df.drop_duplicates(inplace=True, keep=False)

        if mg_df.shape[0] > 0:
            db['summary_tables_mg'].insert_many(mg_df.to_dict('records'))
        else:
            print('no new mg data to add')
        if pct_df.shape[0] > 0:
            db['summary_tables_pct'].insert_many(pct_df.to_dict('records'))
        else:
            print('no new pct data to add')

        print('data storage successful')

        conn.close()


if __name__ == '__main__':
    scraper = main_page_scraper()
    # df = scraper.extract_all_tables()
    scraper.store_mg_pct_dfs()
