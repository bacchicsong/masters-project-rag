import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import json
import shutil

from pathlib import Path
from typing import Set, List, Dict
from tqdm import tqdm
from colorama import init, Fore, Style
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
import re
import os


load_dotenv('/opt/airflow/.env')

def clean_json(data):
    cleaned_data = data.copy()

    cleaned_data.pop('url', None)
    cleaned_data.pop('content', None)

    return cleaned_data


def clean_json_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    cleaned_data = clean_json(data)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

def send_email(subject, body):
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.yandex.ru')
    SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
    SENDER_EMAIL = os.getenv('SMTP_USER', 'leonidvaschin@yandex.ru')
    PASSWORD = os.getenv('SMTP_PASSWORD', '')
    RECEIVER_EMAIL = os.getenv('SMTP_RECEIVER', 'leonidvaschin@yandex.ru')

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, PASSWORD)
            server.send_message(msg)
        print(f"Email отправлен: {subject}")
        return True
    except Exception as e:
        print(f"Ошибка отправки email: {e}")
        return False

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

class ParserConfig:
    BASE_URL = "https://www.tbank.ru/invest/help/educate/how-it-works/"
    OUTPUT_DIR = "tbank_knowledge"

    MAX_RECURSION_DEPTH = 4

    URL_INCLUDE_PATTERNS = ['/invest/help/']
    URL_EXCLUDE_PATTERNS = ['/login', '/logout', '/api/', '.pdf', '.jpg', '.png', '#']

    REQUEST_DELAY = 1.0
    REQUEST_TIMEOUT = 10

    MIN_PARAGRAPH_LENGTH = 10

    CHECKPOINT_ENABLED = True
    CHECKPOINT_INTERVAL = 10

    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'


# ============================================================================
# ПАРСЕР
# ============================================================================

class TBankKnowledgeParser:
    def __init__(self, config: ParserConfig = ParserConfig):
        self.config = config
        self.output_dir = Path(config.OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)

        self.visited_urls: Set[str] = set()
        self.articles: List[Dict] = []
        self.failed_urls: List[str] = []

        self.headers = {'User-Agent': config.USER_AGENT}
        self.allowed_domain = urlparse(config.BASE_URL).netloc

        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_links_found': 0,
            'total_articles': 0,
            'total_chars': 0,
            'start_time': None
        }

        print(f"\n{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}🚀 T-Bank Knowledge Parser")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}📁 {self.output_dir}")
        print(f"{Fore.GREEN}🌐 {config.BASE_URL}")
        print(f"{Fore.YELLOW}📊 Глубина: {config.MAX_RECURSION_DEPTH}\n")

    def get_page_content(self, url: str) -> BeautifulSoup:
        self.stats['total_requests'] += 1

        try:
            print(f"{Fore.YELLOW}⬇️  {url}")
            response = requests.get(url, headers=self.headers, timeout=self.config.REQUEST_TIMEOUT)
            response.raise_for_status()

            self.stats['successful_requests'] += 1
            print(f"{Fore.GREEN}✅ {len(response.content)} байт")

            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            self.stats['failed_requests'] += 1
            self.failed_urls.append(url)
            print(f"{Fore.RED}❌ {str(e)[:100]}")
            return None

    def is_valid_url(self, url: str) -> bool:
        for pattern in self.config.URL_EXCLUDE_PATTERNS:
            if pattern in url:
                return False
        for pattern in self.config.URL_INCLUDE_PATTERNS:
            if pattern in url:
                return True
        return False

    def extract_links(self, soup: BeautifulSoup, current_url: str) -> Set[str]:
        links = set()
        if not soup:
            return links

        all_links = soup.find_all('a', href=True)

        for link in tqdm(all_links, desc="Фильтрация ссылок", leave=False):
            href = link['href']
            full_url = urljoin(current_url, href)

            if urlparse(full_url).netloc != self.allowed_domain:
                continue

            if not self.is_valid_url(full_url):
                continue

            if full_url not in self.visited_urls:
                links.add(full_url)

        self.stats['total_links_found'] += len(links)
        print(f"{Fore.GREEN}   Новых ссылок: {len(links)}")

        return links

    def extract_article_content(self, soup: BeautifulSoup, url: str) -> Dict:
        if not soup:
            return None

        article_data = {
            'url': url,
            'title': '',
            'content': '',
            'sections': []
        }

        title = soup.find('h1')
        if title:
            article_data['title'] = title.get_text(strip=True)

        content_selectors = [
            {'class': 'article-content'},
            {'class': 'content'},
            {'role': 'main'},
            {'class': 'text-content'},
        ]

        main_content = None
        for selector in content_selectors:
            main_content = soup.find('div', selector) or soup.find('main', selector)
            if main_content:
                break

        if not main_content:
            main_content = soup.find('body')

        if main_content:
            paragraphs = main_content.find_all(['p', 'li', 'h2', 'h3', 'h4'])
            content_parts = []

            for elem in paragraphs:
                text = elem.get_text(strip=True)
                if text and len(text) > self.config.MIN_PARAGRAPH_LENGTH:
                    content_parts.append(text)

            article_data['content'] = '\n\n'.join(content_parts)
            self.stats['total_chars'] += len(article_data['content'])

            headings = main_content.find_all(['h2', 'h3'])
            for heading in headings:
                section = {
                    'heading': heading.get_text(strip=True),
                    'content': []
                }

                for sibling in heading.find_next_siblings():
                    if sibling.name in ['h2', 'h3']:
                        break
                    if sibling.name in ['p', 'ul', 'ol']:
                        text = sibling.get_text(strip=True)
                        if text:
                            section['content'].append(text)

                if section['content']:
                    article_data['sections'].append(section)

            print(f"{Fore.GREEN}   {len(content_parts)} параграфов, {len(article_data['content'])} символов")

        return article_data

    def sanitize_filename(self, text: str, max_length: int = 100) -> str:
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '_', text)
        text = re.sub(r'_+', '_', text)
        text = text[:max_length].strip('_')
        return text.lower()

    def generate_filename(self, article: Dict, index: int) -> str:
        if article.get('title'):
            base_name = self.sanitize_filename(article['title'])
            if base_name:
                return f"{index:03d}_{base_name}.json"

        url_path = urlparse(article['url']).path
        url_parts = [p for p in url_path.split('/') if p]
        if url_parts:
            base_name = self.sanitize_filename(url_parts[-1])
            if base_name:
                return f"{index:03d}_{base_name}.json"

        return f"{index:03d}_article.json"

    def save_article(self, article: Dict, index: int):
        filename = self.generate_filename(article, index)
        filepath = self.output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(article, f, ensure_ascii=False, indent=2)

        print(f"{Fore.CYAN}💾 {filename}")

    def save_checkpoint(self):
        if not self.config.CHECKPOINT_ENABLED:
            return

        if len(self.articles) % self.config.CHECKPOINT_INTERVAL == 0 and len(self.articles) > 0:
            checkpoint_file = f"checkpoint_{len(self.articles)}_articles.json"
            checkpoint_path = self.output_dir / checkpoint_file

            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(self.articles, f, ensure_ascii=False, indent=2)

            elapsed = time.time() - self.stats['start_time']
            rate = (len(self.articles) / elapsed) * 60 if elapsed > 0 else 0

            print(f"\n{Fore.YELLOW}💾 Checkpoint: {len(self.articles)} статей ({rate:.1f}/мин)\n")

    def parse_page(self, url: str, depth: int = 0, max_depth: int = None, pbar: tqdm = None):
        if max_depth is None:
            max_depth = self.config.MAX_RECURSION_DEPTH

        if url in self.visited_urls or depth > max_depth:
            return

        print(f"\n{Fore.CYAN}{'  ' * depth}[Глубина {depth}/{max_depth}] {url}")

        self.visited_urls.add(url)

        if len(self.visited_urls) > 1:
            time.sleep(self.config.REQUEST_DELAY)

        soup = self.get_page_content(url)
        article = self.extract_article_content(soup, url)

        if article and article['content']:
            self.articles.append(article)
            self.stats['total_articles'] += 1
            article_index = len(self.articles)

            self.save_article(article, article_index)
            print(f"{Fore.GREEN}✅ Статья #{article_index}: {article['title'][:50]}")

            self.save_checkpoint()

            if pbar:
                pbar.set_postfix({'статей': len(self.articles), 'символов': f"{self.stats['total_chars']:,}"})

        if depth < max_depth:
            links = self.extract_links(soup, url)

            if links:
                for link in links:
                    self.parse_page(link, depth + 1, max_depth, pbar)
                    if pbar:
                        pbar.update(1)

    def save_final(self):
        final_path = self.output_dir / "all_articles.json"

        with open(final_path, 'w', encoding='utf-8') as f:
            json.dump(self.articles, f, ensure_ascii=False, indent=2)

        file_size = final_path.stat().st_size
        print(f"{Fore.GREEN}💾 all_articles.json ({file_size / 1024:.1f} KB, {len(self.articles)} статей)")

    def print_stats(self):
        print(f"\n{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}📊 СТАТИСТИКА ПАРСИНГА")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")

        print(f"\n{Fore.GREEN}Запросы: {self.stats['successful_requests']}/{self.stats['total_requests']}")
        print(f"{Fore.BLUE}Ссылки: {self.stats['total_links_found']} найдено, {len(self.visited_urls)} посещено")
        print(f"{Fore.MAGENTA}Статьи: {self.stats['total_articles']} ({self.stats['total_chars']:,} символов)")

        if self.failed_urls:
            print(f"{Fore.RED}Ошибки: {len(self.failed_urls)} URL")

        print(f"{Fore.CYAN}{'=' * 80}\n")

    def run(self):
        start_time = time.time()
        self.stats['start_time'] = start_time

        with tqdm(total=0, desc="Прогресс парсинга", unit="стр") as pbar:
            self.parse_page(self.config.BASE_URL, pbar=pbar)

        elapsed = time.time() - start_time

        print(f"\n{Fore.GREEN}{'=' * 80}")
        print(f"{Fore.GREEN}✅ ПАРСИНГ ЗАВЕРШЕН за {elapsed:.1f} сек ({elapsed / 60:.1f} мин)")
        print(f"{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}")

        self.print_stats()
        self.save_final()

        return self.articles


# ============================================================================
# ОЧИСТКА ДУБЛИКАТОВ
# ============================================================================

class DirectoryCleaner:
    def __init__(self, directory: Path):
        self.directory = directory
        self.stats = {
            'duplicates_removed': 0,
            'files_renamed': 0,
            'checkpoints_removed': 0,
            'numbered_removed': 0
        }

    def remove_duplicates(self):
        print(f"\n{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}🧹 ШАГ 1: УДАЛЕНИЕ ДУБЛИКАТОВ")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        files_by_url = {}

        for filepath in self.directory.glob("*.json"):
            if filepath.name.startswith(('checkpoint_', 'all_')):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    url = data.get('url', '')

                    if url:
                        if url in files_by_url:
                            filepath.unlink()
                            self.stats['duplicates_removed'] += 1
                            print(f"{Fore.RED}✓ Удален дубликат: {filepath.name}")
                        else:
                            files_by_url[url] = filepath
            except Exception as e:
                print(f"{Fore.RED}Ошибка: {filepath.name} - {e}")

        print(f"\n{Fore.GREEN}Удалено дубликатов: {self.stats['duplicates_removed']}\n")

    def remove_numbering(self):
        print(f"{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}🔄 ШАГ 2: СОЗДАНИЕ ФАЙЛОВ БЕЗ НУМЕРАЦИИ")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        for filepath in self.directory.glob("*.json"):
            if filepath.name.startswith(('checkpoint_', 'all_')):
                continue

            filename = filepath.name

            if len(filename) > 4 and filename[:3].isdigit() and filename[3] == '_':
                new_name = filename[4:]
                new_path = filepath.parent / new_name

                if not new_path.exists():
                    shutil.copy2(filepath, new_path)
                    self.stats['files_renamed'] += 1
                    print(f"{Fore.GREEN}✓ Создан: {new_name}")

        print(f"\n{Fore.GREEN}Создано файлов без нумерации: {self.stats['files_renamed']}\n")

    def remove_numbered_files(self):
        print(f"{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}🗑️  ШАГ 3: УДАЛЕНИЕ ПРОНУМЕРОВАННЫХ ФАЙЛОВ")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        for filepath in list(self.directory.glob("*.json")):
            if filepath.name.startswith(('checkpoint_', 'all_')):
                continue

            filename = filepath.name

            # Если файл начинается с 3 цифр и подчеркивания - удаляем
            if len(filename) > 4 and filename[:3].isdigit() and filename[3] == '_':
                filepath.unlink()
                self.stats['numbered_removed'] += 1
                print(f"{Fore.RED}✓ Удален: {filename}")

        print(f"\n{Fore.GREEN}Удалено пронумерованных файлов: {self.stats['numbered_removed']}\n")

    def remove_checkpoints(self):
        print(f"{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}🗑️  ШАГ 4: УДАЛЕНИЕ ЧЕКПОИНТОВ")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        for filepath in self.directory.glob("checkpoint_*.json"):
            filepath.unlink()
            self.stats['checkpoints_removed'] += 1
            print(f"{Fore.GREEN}✓ Удален: {filepath.name}")

        if self.stats['checkpoints_removed'] == 0:
            print(f"{Fore.YELLOW}Чекпоинты не найдены")
        else:
            print(f"\n{Fore.GREEN}Удалено чекпоинтов: {self.stats['checkpoints_removed']}")

        print()

    def verify_cleanup(self):
        print(f"{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}✅ ПРОВЕРКА РЕЗУЛЬТАТА")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        all_files = [f for f in self.directory.glob("*.json")
                     if not f.name.startswith(('checkpoint_', 'all_'))]

        numbered_files = [f for f in all_files
                          if len(f.name) > 4 and f.name[:3].isdigit() and f.name[3] == '_']

        print(f"{Fore.GREEN}Всего файлов: {len(all_files)}")

        if numbered_files:
            print(f"{Fore.RED}⚠️  ОШИБКА: Остались пронумерованные файлы: {len(numbered_files)}")
            for f in numbered_files[:10]:
                print(f"{Fore.RED}  • {f.name}")
            if len(numbered_files) > 10:
                print(f"{Fore.RED}  ... и еще {len(numbered_files) - 10}")
        else:
            print(f"{Fore.GREEN}✅ Пронумерованных файлов не осталось!")
            print(f"{Fore.GREEN}✅ Все файлы имеют корректные названия!")

        print(f"\n{Fore.CYAN}{'=' * 80}\n")

    def print_stats(self):
        print(f"{Fore.CYAN}{'=' * 80}")
        print(f"{Fore.CYAN}📊 ИТОГОВАЯ СТАТИСТИКА ОЧИСТКИ")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
        print(f"{Fore.GREEN}  • Удалено дубликатов: {self.stats['duplicates_removed']}")
        print(f"{Fore.GREEN}  • Создано файлов без нумерации: {self.stats['files_renamed']}")
        print(f"{Fore.GREEN}  • Удалено пронумерованных: {self.stats['numbered_removed']}")
        print(f"{Fore.GREEN}  • Удалено чекпоинтов: {self.stats['checkpoints_removed']}")
        print(f"\n{Fore.CYAN}{'=' * 80}\n")

    def run(self):
        self.remove_duplicates()
        self.remove_numbering()
        self.remove_numbered_files()
        self.remove_checkpoints()
        self.verify_cleanup()
        self.print_stats()


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    print(f"\n{Fore.CYAN}{'=' * 80}")
    print(f"{Fore.CYAN}T-BANK KNOWLEDGE BASE PARSER & CLEANER")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

    # Этап 1: Парсинг
    print(f"{Fore.YELLOW}📥 ЭТАП 1: ПАРСИНГ\n")
    parser = TBankKnowledgeParser(config=ParserConfig)
    articles = parser.run()

    # Этап 2: Очистка
    print(f"\n{Fore.YELLOW}🧹 ЭТАП 2: ОЧИСТКА\n")
    cleaner = DirectoryCleaner(parser.output_dir)
    cleaner.run()

    # Финальная статистика
    print(f"{Fore.GREEN}{'=' * 80}")
    print(f"{Fore.GREEN}🎉 ВСЕ ЗАВЕРШЕНО!")
    print(f"{Fore.GREEN}{'=' * 80}{Style.RESET_ALL}\n")
    print(f"{Fore.CYAN}📁 Директория: {parser.output_dir.absolute()}")
    print(f"{Fore.CYAN}📚 Уникальных статей: {len(articles)}\n")


if __name__ == "__main__":
    main()