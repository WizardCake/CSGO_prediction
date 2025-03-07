import undetected_chromedriver as uc
import random
import json
import time
import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Lock global para garantir que apenas uma thread crie o driver por vez
driver_creation_lock = Lock()

# Lista para rotação de User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.89 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
]

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

def create_browser(max_retries: int = 3):
    """
    Creates an undetected_chromedriver browser instance.
    Implements retry logic with exponential backoff.
    Manually specifies the Chrome version with `version_main`.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with driver_creation_lock:
                options = uc.ChromeOptions()
                options.add_argument("--incognito")
                options.add_argument(f"user-agent={get_random_user_agent()}")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--disable-popup-blocking")
                options.add_argument("--no-first-run --no-service-autorun --password-store=basic")
                browser = uc.Chrome(options=options, use_subprocess=True)
            print("\nBrowser created successfully.")
            return browser
        except Exception as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt < max_retries:
                sleep_time = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                raise Exception(f"\nFailed to create browser after {max_retries} attempts") from e

def human_scroll(browser) -> None:
    """Simula o comportamento de scroll humano com espera reduzida."""
    for _ in range(random.randint(1, 3)):
        scroll_height = random.randint(300, 800)
        browser.execute_script(f"window.scrollBy(0, {scroll_height});")
        time.sleep(random.uniform(0.5, 1.5))

def human_interaction(browser) -> None:
    """Simula interação humana na página com espera reduzida."""
    try:
        body = browser.find_element(By.TAG_NAME, "body")
        ActionChains(browser).move_to_element(body).perform()
        body.send_keys(Keys.ARROW_DOWN)
        time.sleep(random.uniform(1, 2))
    except Exception:
        pass
    
def get_data_unix(browser) -> Dict[str, Optional[str]]:
    """Extrai o atributo data-unix do elemento localizado pelo XPath e o converte para data."""
    try:
        element = browser.find_element(
            By.XPATH, 
            '/html/body/div[5]/div[8]/div[2]/div[1]/div[2]/div[2]/div[2]/div[2]'
        )
        data_unix = element.get_attribute("data-unix")
        converted_date = None
        try:
            timestamp = int(data_unix)
            # Se o timestamp possuir mais de 10 dígitos, assume que está em milissegundos
            if len(data_unix) > 10:
                timestamp = timestamp / 1000.0
            converted_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as conv_err:
            print("Erro ao converter data-unix:", conv_err)
        return {"data_unix": data_unix, "data_unix_converted": converted_date}
    except Exception as e:
        print("Erro ao extrair data-unix:", e)
        return {"data_unix": None, "data_unix_converted": None}

def get_date(browser) -> Dict[str, Optional[str]]:
    """Retorna a data exibida no topo da página."""
    try:
        return {"date": browser.find_element(By.CLASS_NAME, "date").text}
    except Exception:
        return {"date": None}

def get_flexbox(browser) -> Dict[str, any]:
    """Extrai dados dos times, placares e resultado do mapa."""
    flexbox_dict = {}
    box = browser.find_element(By.CLASS_NAME, "flexbox-column")
    team_names = box.find_elements(By.CLASS_NAME, "results-teamname")
    flexbox_dict["first_team"] = team_names[0].text
    flexbox_dict["second_team"] = team_names[1].text

    first_team_text = browser.find_element(By.CLASS_NAME, "team1-gradient").text.strip()
    second_team_text = browser.find_element(By.CLASS_NAME, "team2-gradient").text.strip()

    flexbox_dict["first_team_total_score"] = int(first_team_text.split("\n")[-1])
    flexbox_dict["second_team_total_score"] = int(second_team_text.split("\n")[-1])
    flexbox_dict["first_team_won"] = int(
        flexbox_dict["first_team_total_score"] > flexbox_dict["second_team_total_score"]
    )
    return flexbox_dict

def get_picks_bans(browser, first_team: str) -> Dict[str, any]:
    """Extrai os picks e bans dos mapas."""
    picks_bans_dict = {}
    try:
        picks_bans = browser.find_elements(By.CLASS_NAME, "col-6")[1].text.split("\n")
        first_ban = picks_bans[0].split(".")[-1].strip().split(" ")[0].lower()
        picks_bans_dict["first_pick_by_first_team"] = int(first_ban == first_team.lower())
        keys = ["ban 1", "ban 2", "pick 1", "pick 2", "ban 3", "ban 4", "pick 3"]
        for i, key in enumerate(keys):
            if i < len(picks_bans):
                picks_bans_dict[key] = picks_bans[i].split(" ")[-1]
    except IndexError:
        print("Pick/Ban data incomplete.")
    return picks_bans_dict

def get_player_stats(browser) -> List[Dict]:
    """Extrai estatísticas dos jogadores de cada time da partida."""
    stats = []
    try:
        # Aguardar até que o conteúdo completo esteja carregado
        WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.ID, "all-content"))
        )
        # Extração dos nomes dos times via XPath
        team1_name = browser.find_element(By.XPATH, '//*[@id="all-content"]/table[1]/tbody/tr[1]/td[1]/div/a').text.strip()
        team2_name = browser.find_element(By.XPATH, '//*[@id="all-content"]/table[4]/tbody/tr[1]/td[1]/div/a').text.strip()

        # Extração das tabelas de estatísticas para cada time
        team1_table = browser.find_element(By.XPATH, '//*[@id="all-content"]/table[1]')
        team2_table = browser.find_element(By.XPATH, '//*[@id="all-content"]/table[4]')
        tables = [(team1_table, team1_name), (team2_table, team2_name)]

        # Itera sobre as tabelas para extrair estatísticas
        for table, team_name in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows[1:]:  # Ignora a linha de cabeçalho
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) > 1:
                    player = cols[0].text.strip()
                    k_d = cols[1].text.strip()
                    plus_minus = cols[2].text.strip()
                    adr = cols[3].text.strip()
                    kast = cols[4].text.strip()
                    rating = cols[5].text.strip()
                    stats.append({
                        "team": team_name,
                        "player": player,
                        "k_d": k_d,
                        "plus_minus": plus_minus,
                        "adr": adr,
                        "kast": kast,
                        "rating": rating
                    })
        return stats
    except Exception as e:
        print(f"Erro ao extrair estatísticas dos jogadores: {e}")
        return []

def process_url(url: str) -> Optional[Dict]:
    """
    Processa uma única URL:
      - Cria seu próprio navegador (com lock na criação)
      - Extrai os dados da partida (data, times, placares, picks/bans, data-unix convertido) e as estatísticas dos jogadores
      - Se nenhum dado for extraído (exceto url e match_id), retorna None
      - Fecha o navegador e retorna um dicionário com os detalhes
    """
    browser = create_browser()

    def is_empty_value(val) -> bool:
        if val is None:
            return True
        if isinstance(val, str) and val.strip() == "":
            return True
        if isinstance(val, list) and len(val) == 0:
            return True
        if isinstance(val, dict) and len(val) == 0:
            return True
        return False

    try:
        browser.get(url)
        # Aguardar carregamento dos elementos chave
        WebDriverWait(browser, random.randint(2, 4)).until(
            EC.presence_of_element_located((By.CLASS_NAME, "date"))
        )
        human_scroll(browser)
        human_interaction(browser)

        # Coleta a data e o data-unix convertido
        details_dict = get_date(browser)
        details_dict.update(get_data_unix(browser))
        
        flex = get_flexbox(browser)
        details_dict.update(flex)
        picks = get_picks_bans(browser, flex["first_team"])
        details_dict.update(picks)

        # Coleta as estatísticas dos jogadores
        player_stats = get_player_stats(browser)
        details_dict["player_stats"] = player_stats

        details_dict["url"] = url
        details_dict["match_id"] = hash(url)  # Identificador único para rastreamento

        # Verifica se ao menos um campo (exceto url e match_id) possui dado relevante
        data_extracted = False
        for key, value in details_dict.items():
            if key in ("url", "match_id"):
                continue
            if not is_empty_value(value):
                data_extracted = True
                break
        if not data_extracted:
            print(f"Skipping match {url} because no data was extracted.")
            return None

        time.sleep(random.uniform(3, 7))
        return details_dict
    except Exception as e:
        print(f"Skipping match {url} due to error: {e}")
        return None
    finally:
        try:
            browser.quit()
        except Exception:
            pass

def chunker(seq: List, size: int) -> List[List]:
    """Divide a lista em sublistas de tamanho 'size'."""
    return [seq[pos:pos + size] for pos in range(0, len(seq), size)]

def extract_players(csv_filename="data/transformacao_intermediaria.csv", sep=";"):
    """
    Extrai os detalhes dos jogos:
      - Lê o input (CSV) e o output (JSON existente) para determinar quais URLs ainda não foram processadas.
      - Processa em batches de 15 URLs faltantes.
      - Utiliza até 4 tarefas paralelas para processar cada batch.
      - Atualiza o arquivo JSON com os novos dados.
    """
    # Carrega o CSV de input
    data = pd.read_csv(csv_filename, sep=sep, index_col=0)
    url_list = list(data.index)

    # Tenta ler o arquivo JSON existente para identificar URLs já processadas
    processed_details = []
    processed_urls = set()
    if os.path.exists("data/match_details.json"):
        try:
            with open("data/match_details.json", "r") as f:
                processed_details = json.load(f)
            processed_urls = {match["url"] for match in processed_details if "url" in match}
            print(f"Encontrados {len(processed_urls)} jogos já processados. Serão ignorados.")
        except Exception as e:
            print(f"Erro ao ler data/match_details.json: {e}. Processando todos os jogos.")

    # Filtra os URLs para processar somente os que ainda não foram extraídos
    missing_urls = [url for url in url_list if url not in processed_urls]
    print(f"{len(missing_urls)} jogos serão processados nesta execução.")

    batches = chunker(missing_urls, 15)

    overall_progress = tqdm(batches, desc="Processando batches", unit="batch")
    for batch in overall_progress:
        batch_details = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(tqdm(executor.map(process_url, batch), total=len(batch), desc="Processando jogos", unit="jogo"))
        for res in results:
            if res is not None:
                batch_details.append(res)
        processed_details.extend(batch_details)
        with open("data/match_details.json", "w") as f:
            json.dump(processed_details, f, indent=4)
        print(f"\nBatch com {len(batch)} jogos processados e salvos.\n")

    print("Extração de dados completa. Resultados salvos em data/match_details.json.")

if __name__ == "__main__":
    extract_players()
