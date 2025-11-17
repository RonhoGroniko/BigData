import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import Counter
import re
import sqlite3


URLS = [
    "https://ru.wikipedia.org/wiki/%D0%91%D0%BE%D0%BB%D1%8C%D1%88%D0%B8%D0%B5_%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5",
    "https://ru.wikipedia.org/wiki/MapReduce",
    "https://ru.wikipedia.org/wiki/Python",
    "https://ru.wikipedia.org/wiki/Valorant",
    "https://ru.wikipedia.org/wiki/Valorant_Champions_Tour",
    "https://ru.wikipedia.org/wiki/Fnatic",
    "https://ru.wikipedia.org/wiki/Gambit_Esports"
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

DB_PATH = "search.db"


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.text


def extract_text_and_links(base_url: str, html: str):

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text()
    text = re.sub(r"\s+", " ", text).strip()

    tokens = re.findall(r"[а-яА-Яa-zA-Z0-9]+", text.lower())
    word_counts = Counter(tokens)

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        links.append(full_url)

    return word_counts, links


def init_db(db_path: str = DB_PATH):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        url  TEXT UNIQUE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS words (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        word TEXT UNIQUE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS doc_words (
        doc_id  INTEGER,
        word_id INTEGER,
        count   INTEGER,
        PRIMARY KEY (doc_id, word_id),
        FOREIGN KEY (doc_id) REFERENCES documents(id),
        FOREIGN KEY (word_id) REFERENCES words(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS links (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        from_doc_id INTEGER,
        to_url      TEXT,
        FOREIGN KEY (from_doc_id) REFERENCES documents(id)
    )
    """)

    conn.commit()

    return conn


def get_or_create_document(cur, url: str) -> int:

    cur.execute("SELECT id FROM documents WHERE url = ?", (url,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO documents (url) VALUES (?)", (url,))
    return cur.lastrowid


def get_or_create_word(cur, word: str) -> int:

    cur.execute("SELECT id FROM words WHERE word = ?", (word,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO words (word) VALUES (?)", (word,))
    return cur.lastrowid


def save_to_db(documents: dict, db_path: str = DB_PATH):

    conn = init_db(db_path)
    cur = conn.cursor()

    for url, data in documents.items():

        print("Сохраняем документ:", url)

        doc_id = get_or_create_document(cur, url)

        for word, count in data["words"].items():
            word_id = get_or_create_word(cur, word)
            cur.execute("""
                INSERT OR REPLACE INTO doc_words (doc_id, word_id, count)
                VALUES (?, ?, ?)
            """, (doc_id, word_id, count))

        for link in data["links"]:
            cur.execute("""
                INSERT INTO links (from_doc_id, to_url)
                VALUES (?, ?)
            """, (doc_id, link))

    conn.commit()
    conn.close()


def build_documents(urls):
    documents = {}
    for url in urls:
        print("Парсим:", url)
        try:
            html = fetch_html(url)
        except Exception as e:
            print(f"Ошибка при загрузке {url}: {e}")
            continue

        word_counts, links = extract_text_and_links(url, html)

        documents[url] = {
            "words": word_counts,
            "links": links
        }

    return documents


def main():
    docs = build_documents(URLS)
    if not docs:
        print("Не удалось спарсить ни один документ.")
        return

    save_to_db(docs, db_path=DB_PATH)
    print("Готово. База данных успешно заполнена:", DB_PATH)


if __name__ == "__main__":
    main()
