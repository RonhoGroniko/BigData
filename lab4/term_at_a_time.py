import sqlite3
import math
import re
from collections import defaultdict

DB_PATH = "search.db"


def load_index(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id, url FROM documents")
    id_to_url = {}
    for doc_id, url in cur.fetchall():
        id_to_url[doc_id] = url
    num_docs = len(id_to_url)

    cur.execute("""
        SELECT w.word, dw.doc_id, dw.count
        FROM doc_words AS dw
        JOIN words AS w ON w.id = dw.word_id
    """)

    index = defaultdict(list)
    for word, doc_id, cnt in cur.fetchall():
        index[word].append((doc_id, cnt))

    for term in index:
        index[term].sort(key=lambda x: x[0])

    conn.close()
    return index, id_to_url, num_docs


def validate(text):
    return re.findall(r"[а-яА-Яa-zA-Z0-9]+", text.lower())


def search_taat(query, index, id_to_url, num_docs, top_k=10):
    terms = validate(query)
    if not terms:
        print("Пустой запрос.")
        return

    scores = defaultdict(float)

    for term in terms:
        docs_for_term = index.get(term)
        if not docs_for_term:
            continue

        df = len(docs_for_term)
        if df == 0:
            continue

        idf = math.log(float(num_docs) / df)

        for doc_id, tf in docs_for_term:
            scores[doc_id] += tf * idf

    if not scores:
        print("Нет документов по запросу.")
        return

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    print(f"Результаты (term-at-a-time) для запроса: {query!r}")
    for doc_id, score in ranked:
        url = id_to_url.get(doc_id, "<unknown>")
        print(f"doc_id={doc_id:3d}  score={score:.4f}  url={url}")


def main():
    index, id_to_url, num_docs = load_index(DB_PATH)
    print("Полнотекстовый поиск (term-at-a-time).")
    query = input("Введите запрос: ")
    search_taat(query, index, id_to_url, num_docs, top_k=10)


if __name__ == "__main__":
    main()
