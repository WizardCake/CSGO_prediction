import undetected_chromedriver as uc
import random
import time
import json
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Monkey-patch para suprimir erros no encerramento do driver
_original_quit = uc.Chrome.quit
def safe_quit(self):
    try:
        _original_quit(self)
    except Exception:
        pass
uc.Chrome.quit = safe_quit

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def random_sleep(min_seconds=0.75, max_seconds=2.3):
    """Pausa aleatória para simular comportamento humano e acelerar o scraping."""
    time.sleep(random.uniform(min_seconds, max_seconds))

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--start-maximized")
    return uc.Chrome(options=options)

def wait_for_element(driver, by, identifier, timeout=8):
    """Espera explícita por um elemento na página usando o driver fornecido."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, identifier))
    )

def get_results_from_page(url):
    """Cria uma instância do navegador, coleta os dados da página e a encerra."""
    driver = get_driver()
    try:
        logging.info(f"Iniciando scraping da página: {url}")
        driver.get(url)
        random_sleep()
        wait_for_element(driver, By.CLASS_NAME, 'a-reset', timeout=10)
        games = driver.find_elements(By.CLASS_NAME, 'a-reset')
        results = [{"jogo": game.text, "link": game.get_attribute('href')} for game in games]
        logging.info(f"Finalizado scraping da página: {url} - {len(results)} resultados encontrados")
    except Exception as e:
        logging.error(f"Erro na página {url}: {e}")
        results = []
    finally:
        driver.quit()
    return results

def get_pagination_offsets():
    """Obtém o total de resultados e calcula os offsets para cada página."""
    driver = get_driver()
    base_url = 'https://www.hltv.org/results'
    try:
        driver.get(base_url)
        random_sleep()
        pagination_elem = wait_for_element(driver, By.CLASS_NAME, 'pagination-data', timeout=10)
        total_text = pagination_elem.text[11:]
        total_results = int(total_text)
        logging.info(f"Total de resultados encontrados: {total_results}")
    except Exception as e:
        logging.error(f"Erro ao obter total de resultados: {e}")
        total_results = 0
    finally:
        driver.quit()
    page_size = 100
    num_pages = (total_results + page_size - 1) // page_size  # Divisão arredondada para cima
    offsets = [i * page_size for i in range(num_pages)]
    logging.info(f"Número de páginas calculadas: {len(offsets)}")
    return offsets

def write_batch_to_file(batch, filename='teste_resultados_partial.ndjson'):
    """
    Salva cada registro do batch em uma linha (NDJSON) no arquivo,
    utilizando o modo 'append'.
    """
    with open(filename, 'a', encoding='utf-8') as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    logging.info(f"Batch de {len(batch)} resultados salvos.")

def load_existing_links(filename='teste_resultados_partial.ndjson'):
    """
    Carrega os links já presentes no arquivo salvo para evitar duplicatas.
    """
    links = set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if 'link' in record:
                        links.add(record['link'])
                except Exception as e:
                    logging.error(f"Erro ao processar linha do arquivo: {e}")
    except FileNotFoundError:
        logging.info("Arquivo não encontrado. Iniciando com conjunto vazio de links.")
    logging.info(f"Registros existentes carregados: {len(links)}")
    return links

def main():
    filename = 'data/extração_partidas.ndjson'
    existing_links = load_existing_links(filename)
    
    base_url = 'https://www.hltv.org/results'
    offset_url = 'https://www.hltv.org/results?offset='
    offsets = get_pagination_offsets()
    # Constrói as URLs: a página base para offset 0 e as demais com o offset
    urls = [base_url if offset == 0 else f"{offset_url}{offset}" for offset in offsets]
    
    batch_size = 5  # Salva a cada 5 páginas processadas
    batch_results = []
    pages_processed = 0
    total_urls = len(urls)
    
    # Processa as páginas de forma sequencial para manter a ordem
    for url in urls:
        results = get_results_from_page(url)
        new_results = [record for record in results if record['link'] not in existing_links]
        
        # Se não houver registros novos, encerra o scraping
        if not new_results:
            logging.info(f"Nenhum novo dado encontrado na página {url}. Encerrando o scraping.")
            break
        
        # Atualiza os registros já existentes e acumula os novos resultados
        for record in new_results:
            existing_links.add(record['link'])
        batch_results.extend(new_results)
        
        pages_processed += 1
        logging.info(f"Progresso em {100*pages_processed/total_urls:.2f}%: {pages_processed}/{total_urls} páginas processadas")
        
        if pages_processed % batch_size == 0:
            write_batch_to_file(batch_results, filename)
            batch_results = []
    
    if batch_results:
        write_batch_to_file(batch_results, filename)
    
    logging.info("Scraping concluído.")

if __name__ == '__main__':
    main()
