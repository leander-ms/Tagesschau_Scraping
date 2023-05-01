from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import warnings
from selenium.webdriver.common.by import By
import time
import pandas as pd
import fnmatch
warnings.filterwarnings("ignore", category=DeprecationWarning) 

DRIVER_PATH = 'C:\\Users\\leand\\Downloads\\chromedriver_win32\\chromedriver.exe'
driver = webdriver.Chrome(executable_path=DRIVER_PATH)
options = Options()
options.headless = True
options.add_argument("--window-size=1920, 1200")
options.add_argument('log-level=3')

def Tagesschau_Scraping(link):
    text_article = driver.find_elements(By.CSS_SELECTOR, 'p.m-ten.m-offset-one.l-eight.l-offset-two.textabsatz.columns.twelve')
    header = driver.find_element(By.CSS_SELECTOR, 'span.seitenkopf__headline--text')
    header = header.text 
    print(header)
    
    whole_article = ''
    for out in text_article:
        whole_article += out.text
    
    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')
    
    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()   
    
    return header, whole_article, timestamp


def NDR_Scraping(link):
    header = driver.find_element(By.CSS_SELECTOR, 'h1')
    header = header.text
    text_article = driver.find_elements(By.CSS_SELECTOR, 'p')
    print(header)
    
    whole_article = ''
    for out in text_article:
        whole_article += out.text
        
    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')
    
    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()  
    
    return header, whole_article, timestamp


def SWR_Scraping(link):
    header = driver.find_element(By.CSS_SELECTOR, 'h1')
    header = header.text
    text_article = driver.find_elements(By.CSS_SELECTOR, 'p')
    print(header)

    whole_article = ''
    for out in text_article:
        whole_article += out.text + ' '
    
    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')
    
    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()  
    
    return header, whole_article, timestamp   


def MDR_Scraping(link):
    header = driver.find_element(By.XPATH, './/span[@class = "headline has-voicereader"]')
    #header = driver.find_element(By.XPATH, './/head[@title]')
    header = header.text
    print(header)
    #header.removesuffix('LESEN')
    header[:-7]
    text_article = driver.find_elements(By.CSS_SELECTOR, 'p')
    
    whole_article = ''
    for out in text_article:
        whole_article += out.text
    
    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')
    whole_article.replace('\n', ' ')
    header.replace('\n', '').replace('\r', '')
    
    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()  
    
    
    return header, whole_article, timestamp

def Sportschau_Scraping(link):
    header = driver.find_element(By.CSS_SELECTOR, 'span.seitenkopf__headline--text')
    text_article = driver.find_elements(By.CSS_SELECTOR, 'p.m-ten.m-offset-one.l-eight.l-offset-two.textabsatz.columns.twelve')
    header = header.text

    whole_article = ''

    for out in text_article:
        whole_article += out.text

    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')

    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()   
    
    return header, whole_article, timestamp


def WDR_Scraping(link):
    header = driver.find_element(By.CSS_SELECTOR, 'h1')
    header = header.text
    text_article = driver.find_elements(By.CSS_SELECTOR, 'div.con')
    print(header)
    
    whole_article = ''
    
    for out in text_article:
        whole_article += out.text
        
    timestamp = time.strftime('%d.%m.%Y %H:%M:%S')
    whole_article.rstrip()
    
    data = {'Headline': [header], 'article_text': [whole_article], 'link_article': [link], 'timestamp': [timestamp]}

    df = pd.DataFrame(data)
    df['article_text'].astype('string')

    check = pd.read_csv('database\\Query_Tagesschau_v3', sep=';', decimal = ',', encoding='UTF8')
    df_check = pd.DataFrame(check)
    if len(df_check)>0:
        if df['Headline'].iloc[-1] != df_check[df_check.columns[0]].iloc[-1]:    
            pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
            print(df)
        else:
            print('No new Data was found, hence no input into csv file.')

        time.sleep(10)
    else:
        pd.DataFrame(df).to_csv('database\\Query_Tagesschau_v3', 
                                    mode='a', index=False, header=False, sep=';', decimal=',', encoding='UTF8')
        print('Init dataset inserted')
        time.sleep(10)

    driver.quit()  
    
    
    return header, whole_article, timestamp

for i in range(100000):

    driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
    driver.get("https://www.tagesschau.de/allemeldungen/")
    #print(driver.page_source)


    res = driver.find_element(By.CSS_SELECTOR, 'div.linklist.withnolinkcontentbutnoproblem [href]')
    link_res = res.get_attribute('href')
    res = res.text
    #print(link_res)


    driver.get(link_res)
    #print(link_res)
    
    
    if fnmatch.fnmatch(link_res, '*tagesschau.de*') == True:
        print('Tagesschau Mode')
        Tagesschau_Scraping(link_res)        
    elif fnmatch.fnmatch(link_res, '*ndr.de*') == True:
        print('NDR Mode')
        NDR_Scraping(link_res)        
    elif fnmatch.fnmatch(link_res, '*mdr.de*') == True:
        print('MDR Mode')
        MDR_Scraping(link_res)        
    elif fnmatch.fnmatch(link_res, '*swr.de*') == True:
        print('SWR Mode')
        SWR_Scraping(link_res)        
    elif fnmatch.fnmatch(link_res, '*wdr.de*') == True:
        print('WDR Mode')
        WDR_Scraping(link_res) 
    elif fnmatch.fnmatch(link_res, '*sportschau.de*') == True:
        print('Sportschau Mode')
        Sportschau_Scraping(link_res)
    else:
        print('Web Adress could not be identified, hence no download/input!')
