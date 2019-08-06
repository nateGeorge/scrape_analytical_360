import re

from pymongo import MongoClient
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.common.exceptions import NoSuchElementException

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
    def __init__(self, proxy=None):
        self.ua = UserAgent()
        self.proxy = proxy
        self.driver = self.setup_driver()
        self.main_url = 'http://analytical360.com/testresults'
        self.pages = ['Flower', 'Concentrate', 'Edible', 'Liquid', 'Topical']
        self.main_url = 'https://analytical360.com/testresults?perpage=1000&tab={}'


    def setup_driver(self):
        if self.proxy is not None:
            http_proxy  = self.proxy#"ip_addr:port"
            https_proxy = self.proxy#"ip_addr:port"

            webdriver.DesiredCapabilities.FIREFOX['proxy']={
                "httpProxy":http_proxy,
                "sslProxy":https_proxy,
                "proxyType":"MANUAL"
            }

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
        if current_mg_data.shape[0] > 0:
            mg_df = mg_df.append(current_mg_data, sort=False)
            mg_df.drop_duplicates(inplace=True, keep=False)
        if current_pct_data.shape[0] > 0:
            pct_df = pct_df.append(current_pct_data, sort=False)
            pct_df.drop_duplicates(inplace=True, keep=False)

        if mg_df.shape[0] > 0:
            mg_df['scraped'] = False
            db['summary_tables_mg'].insert_many(mg_df.to_dict('records'))
        else:
            print('no new mg data to add')
        if pct_df.shape[0] > 0:
            mg_df['scraped'] = False
            db['summary_tables_pct'].insert_many(pct_df.to_dict('records'))
        else:
            print('no new pct data to add')

        print('data storage successful')

        conn.close()




if __name__ == '__main__':
    scraper = main_page_scraper()
    # df = scraper.extract_all_tables()
    # Stores summary tables on main page.  Used to get links to detail pages.
    scraper.store_mg_pct_dfs()


    conn = MongoClient()
    db = conn['analytical360']
    current_mg_data = pd.DataFrame(list(db['summary_tables_mg'].find()))#{}, {'_id': False})))
    current_pct_data = pd.DataFrame(list(db['summary_tables_pct'].find()))
    current_mg_data.set_index('_id', inplace=True)
    current_pct_data.set_index('_id', inplace=True)
    current_mg_data['pct'] = False
    current_pct_data['pct'] = True

    full_data_df = current_pct_data.append(current_mg_data)

    # was able to scrape a few hundred times before getting blocked
    # probably need to put a bigger delay in between visiting pages
    # seems like it blocked the ip
    for i, r in full_data_df.iterrows():
        if r['pct'] is True:
            result = db['summary_tables_pct'].find_one({'_id': i}, {'scraped': 1, '_id': 0})
        else:
            result = db['summary_tables_mg'].find_one({'_id': i}, {'scraped': 1, '_id': 0})

        if result != {}:
            if result['scraped'] is True:
                print('already scraped this page')
                continue

        print('scraping', r['name'], r['link'])
        driver.get(r['link'])
        if r['pct'] is False:
            # click radio button to change units to pct
            driver.find_element_by_id('selectu').find_element_by_id('percent').click()

        try:
            sample_name = driver.find_element_by_id('printSampleName').get_attribute('innerHTML').replace('Sample Name: ', '')
        except NoSuchElementException:
            error404 = driver.find_element_by_class_name('error404')
            if error404 is not None:
                print('404 error, skipping')
                if r['pct'] is True:
                    db['summary_tables_pct'].update_one({'_id': i}, {'$set': {'scraped': True}})
                else:
                    db['summary_tables_mg'].update_one({'_id': i}, {'$set': {'scraped': True}})
                continue

        test_details_text = driver.find_element_by_class_name('ANLheader-left').text.split('\n')
        test_details = {}
        for t in test_details_text:
            try:
                key, val = t.split(': ')
                test_details[key.lower()] = val
            # usually happens if a field was left blank
            except ValueError:
                pass

        test_summary_text = driver.find_element_by_id('summary_table').text.split('\n')
        summary_details = {}
        # one had 'still pending' and ended up being empty
        if test_summary_text != ['']:
            for t in test_summary_text:
                key, val = t.split(': ')
                summary_details[key.lower()] = val
        else:
            if r['pct'] is True:
                db['summary_tables_pct'].update_one({'_id': i}, {'$set': {'scraped': True}})
            else:
                db['summary_tables_mg'].update_one({'_id': i}, {'$set': {'scraped': True}})
            continue


        potency_data = {}
        try:
            potency_table = driver.find_element_by_id('potency')
            for row in potency_table.find_elements_by_tag_name('tr'):
                if 'Cannabinoid totals are adjusted to account for' in row.text:
                    continue

                cols = row.find_elements_by_tag_name('td')
                potency_data[cols[0].text.strip().replace('\n', '').replace('1', '').replace('2', '').lower()] = cols[1].text.strip()
        except NoSuchElementException:
            print('no potency data')

        # doesn't seem to important and in summary table
        # driver.find_element_by_id('moisture')
        # driver.find_element_by_id('foreign')

        terp_data = {}
        try:
            terp_data_table = driver.find_element_by_id('terpenes')
            for t in terp_data_table.find_elements_by_tag_name('tr'):
                cols = t.find_elements_by_tag_name('td')
                terp_data[cols[0].text.strip().lower()] = cols[1].text.strip()
        except NoSuchElementException:
            print('no terpenes on page')

        full_data = {'sample_name': sample_name,
                    'type': r['type'],
                    'link': r['link'],
                    **test_details,
                    **summary_details,
                    **potency_data,
                    **terp_data}

        for k, v in full_data.items():
            # mongodb can't have periods in key names, so use commas
            if 'ND' in v:
                full_data[k] = 0
            if '%' in v:
                full_data[k] = float(full_data[k].replace('%', ''))
            if '.' in k:
                full_data[k.replace('.', ',')] = v
                del full_data[k]

        result = db['detail_data'].find_one({**full_data})
        if result is None:
            db['detail_data'].insert_one(full_data)
            if r['pct'] is True:
                db['summary_tables_pct'].update_one({'_id': i}, {'$set': {'scraped': True}})
            else:
                db['summary_tables_mg'].update_one({'_id': i}, {'$set': {'scraped': True}})
        else:
            print('already scraped this page')



    # load and clean detail data
def create_clean_dataset():
    conn = MongoClient()
    db = conn['analytical360']
    result = list(db['detail_data'].find())
    df = pd.DataFrame(result)

    # remove weird alphanumeric tails from some names
    df.loc[df['sample_name'].str.contains(' IN'), 'sample_name'] = df['sample_name'].apply(lambda x: re.sub(' IN*.', '', x))
    good_names = df[df['sample_name'].apply(lambda x: len(x) > 5) & ~df['sample_name'].str.contains('#') & ~df['sample_name'].str.contains('Distillate')]['sample_name'].values
    np.random.seed(42)  # set seed for reproducible results
    for i, r in df.iterrows():
        # a few names with only one number
        if len(r['sample_name']) <= 2 or 'SS-' in r['sample_name'] or 'Distillate' in r['sample_name']:
            sample_names = set(df['sample_name'].values)
            count = 2
            while True:
                random_name = np.random.choice(good_names) + ' #{}'.format(count)
                if random_name not in sample_names:
                    break
                else:
                    count += 1

            df.loc[i, 'sample_name'] = random_name

    columns = ['sample_name', 'type', 'alpha pinene', 'beta pinene', 'caryophyllene', 'cbc', 'cbd', 'cbd total (cbd-a * 0,877 + cbd)', 'cbdv total (cbdv-a * 0,878 + cbdv)', 'cbg total (cbg-a * 0,878 + cbg)', 'cbn', 'thc total (thc-a * 0,877 + thc)', 'humulene', 'limonene', 'linalool', 'myrcene', 'ocimene', 'terpinolene']
    renamed_columns = ['name', 'type', 'alpha_pinene', 'beta_pinene', 'caryophyllene', 'cbc', 'cbd', 'cbd_total', 'cbdv_total', 'cbg_total', 'cbn', 'thc_total', 'humulene', 'limonene', 'linalool', 'myrcene', 'ocimene', 'terpinolene']

    smaller_df = df[columns].copy()
    smaller_df.columns = renamed_columns
    terp_df = smaller_df[smaller_df['ocimene'].notna()].copy()

    current_clean_scr_data = pd.DataFrame(list(db['clean_scraped_data'].find({}, {'_id': 0})))
    if current_clean_scr_data.shape[0] > 0:
        full_df = terp_df.append(current_clean_scr_data)
        full_df.drop_duplicates(inplace=True, keep=False)
    else:
        full_df = terp_df

    db['clean_scraped_data'].insert_many(full_df.to_dict('records'))
    conn.close()
