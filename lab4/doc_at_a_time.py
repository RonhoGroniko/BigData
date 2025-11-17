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
    for word, doc_id, count in cur.fetchall():
        index[word].append((doc_id, count))

    for term in index:
        index[term].sort(key=lambda x: x[0])

    conn.close()
    return index, id_to_url, num_docs


def validate(text):
    return re.findall(r"[а-яА-Яa-zA-Z0-9]+", text.lower())


def search_daat(query, index, id_to_url, num_docs, top_k=10):
    terms = validate(query)
    if not terms:
        print("Пустой запрос.")
        return

    term_docs = {}
    for term in terms:
        docs_for_term = index.get(term)
        if docs_for_term:
            term_docs[term] = docs_for_term

    if not term_docs:
        print("Нет документов по запросу.")
        return

    pointers = {term: 0 for term in term_docs.keys()}
    scores = defaultdict(float)

    while True:
        current_doc_ids = []

        for term, docs_for_term in term_docs.items():
            pos = pointers[term]
            if pos < len(docs_for_term):
                doc_id, tf = docs_for_term[pos]
                current_doc_ids.append(doc_id)

        if not current_doc_ids:
            break

        current_doc = min(current_doc_ids)

        for term, docs_for_term in term_docs.items():
            pos = pointers[term]
            if pos >= len(docs_for_term):
                continue

            doc_id, tf = docs_for_term[pos]
            if doc_id == current_doc:
                df = len(docs_for_term)
                if df > 0:
                    idf = math.log(float(num_docs) / df)
                    scores[doc_id] += tf * idf
                pointers[term] += 1

    if not scores:
        print("Нет документов по запросу.")
        return

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    print(f"Результаты (document-at-a-time) для запроса: {query!r}")
    for doc_id, score in ranked:
        url = id_to_url.get(doc_id, "<unknown>")
        print(f"doc_id={doc_id:3d}  score={score:.4f}  url={url}")


def main():
    index, id_to_url, num_docs = load_index(DB_PATH)
    print("Полнотекстовый поиск (document-at-a-time).")
    query = input("Введите запрос: ")
    search_daat(query, index, id_to_url, num_docs, top_k=10)


if __name__ == "__main__":
    main()
