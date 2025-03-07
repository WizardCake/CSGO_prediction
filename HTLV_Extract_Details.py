from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from time import sleep
import json
import pandas as pd

def macro_info(browser, paginas):
    details = []
    
    for pagina in paginas:
        browser.get(pagina)
        sleep(2)  # Remova se não for necessário
        
        try:
            pre_match = browser.find_elements(By.CLASS_NAME, 'padding')
            date = browser.find_element(By.CLASS_NAME, 'date')
        except:
            continue  # Se não encontrar, pula para a próxima página
        
        played_info = [game.text for game in browser.find_elements(By.CLASS_NAME, 'played')]
        list_links = [link.get_attribute('href') for link in browser.find_elements(By.CLASS_NAME, 'results-stats')]
        
        if len(pre_match) < 2:  # Evita erro de índice
            continue
        
        dict_details = {
            'data': date.text,
            'what': pre_match[0].text,
            'PicsBans': pre_match[1].text.split('\n'),
            'Teams_History': pre_match[-1].text.split('\n'),
            'played_info': played_info,
            'list_links': list_links
        }
        
        if len(dict_details['Teams_History']) >= 8:  # Evita erro de índice
            dict_details['Teams_History'] = {
                dict_details['Teams_History'][0]: dict_details['Teams_History'][1],
                'Overtimes': dict_details['Teams_History'][3],
                dict_details['Teams_History'][7]: dict_details['Teams_History'][5]
            }
        
        details.append(dict_details)
    
    return details
