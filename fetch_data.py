import random
import time
import json
import datetime
import threading
import concurrent.futures
import logging
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from tqdm import tqdm

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.89 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
]

# Lock global para a inicialização do driver, evitando conflitos de patching
driver_lock = threading.Lock()

def update_matches_with_date_unix(matches, file_path):
    """
    Processa cada partida em paralelo. Se 'data_unix' estiver vazia,
    extrai o valor do atributo 'data-unix' utilizando Selenium,
    converte o timestamp para 'data_unix_converted' e atualiza o registro.

    O arquivo JSON é salvo a cada 15 partidas atualizadas.
    """
    xpath_expr = '/html/body/div[5]/div[8]/div[2]/div[1]/div[2]/div[2]/div[2]/div[2]'
    updated_count = 0
    count_lock = threading.Lock()
    
    def process_match(match):
        updated = False
        if not match.get("data_unix"):
            user_agent = random.choice(USER_AGENTS)
            options = uc.ChromeOptions()
            options.add_argument(f'--user-agent={user_agent}')
            # Garante que apenas uma thread crie o driver por vez
            with driver_lock:
                driver = uc.Chrome(options=options)
            try:
                driver.get(match["url"])
                time.sleep(random.uniform(3, 6))
                
                # Simula scroll aleatório
                scroll_height = driver.execute_script("return document.body.scrollHeight")
                for _ in range(random.randint(2, 4)):
                    random_position = random.randint(0, scroll_height)
                    driver.execute_script("window.scrollTo(0, arguments[0]);", random_position)
                    time.sleep(random.uniform(1, 3))
                
                element = driver.find_element(By.XPATH, xpath_expr)
                # Extrai o valor do atributo 'data-unix'
                date_unix = element.get_attribute("data-unix")
                match["data_unix"] = date_unix
                logging.info(f"Match {match['match_id']} - Extraído data_unix: {date_unix}")
                updated = True
            except Exception as e:
                logging.error(f"Match {match['match_id']} - Erro: {e}")
            finally:
                driver.quit()
        else:
            logging.info(f"Match {match['match_id']} já possui data_unix: {match['data_unix']}")
        
        if match.get("data_unix") and not match.get("data_unix_converted"):
            try:
                # Converter timestamp (assumindo milissegundos)
                timestamp = int(match["data_unix"]) / 1000.0
                converted_date = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                match["data_unix_converted"] = converted_date
                logging.info(f"Match {match['match_id']} - data_unix_converted atualizado: {converted_date}")
                updated = True
            except Exception as conv_err:
                logging.error(f"Match {match['match_id']} - Erro na conversão da data: {conv_err}")
        return updated

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_match, match): match for match in matches}
        progress_bar = tqdm(total=len(futures), desc="Processando partidas")
        for future in concurrent.futures.as_completed(futures):
            progress_bar.update(1)
            try:
                was_updated = future.result()
                if was_updated:
                    with count_lock:
                        updated_count += 1
                        if updated_count % 15 == 0:
                            with open(file_path, "w", encoding="utf-8") as f:
                                json.dump(matches, f, indent=4, ensure_ascii=False)
                            logging.info(f"Arquivo salvo após {updated_count} partidas atualizadas.")
            except Exception as exc:
                match_err = futures[future]
                logging.error(f"Match {match_err['match_id']} gerou exceção: {exc}")
        progress_bar.close()

    # Grava o arquivo final ao término do processamento
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(matches, f, indent=4, ensure_ascii=False)
    logging.info("Arquivo salvo ao final.")
    return matches

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    file_path = "data/match_details.json"
    
    with open(file_path, "r", encoding="utf-8") as file:
        match_data = json.load(file)
    
    updated_data = update_matches_with_date_unix(match_data, file_path)
    
    print(json.dumps(updated_data, indent=4, ensure_ascii=False))
