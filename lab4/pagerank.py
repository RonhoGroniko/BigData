import sqlite3
from collections import defaultdict

DB_PATH = "search.db"


def load_links(db_path=DB_PATH):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id, url FROM documents")
    id_to_url = {}
    url_to_id = {}
    for doc_id, url in cur.fetchall():
        id_to_url[doc_id] = url
        url_to_id[url] = doc_id

    result = {doc_id: [] for doc_id in id_to_url.keys()}

    cur.execute("SELECT from_doc_id, to_url FROM links")
    for from_id, to_url in cur.fetchall():

        to_id = url_to_id.get(to_url)
        if to_id is not None and from_id in result:
            result[from_id].append(to_id)

    conn.close()
    return result, id_to_url


def mapper(links_dict, ranks):

    contributions = defaultdict(float)
    dangling_mass = 0.0

    for doc_from, neighbors in links_dict.items():
        rank_from = ranks[doc_from]
        if neighbors:
            share = rank_from / len(neighbors)
            for doc_to in neighbors:
                contributions[doc_to] += share
        else:
            dangling_mass += rank_from

    return contributions, dangling_mass


def reducer(links_dict, contributions, dangling_mass, num_pages, alpha=0.85):

    new_ranks = {}
    base = (1.0 - alpha) / num_pages
    dangling_share = alpha * dangling_mass / num_pages

    all_pages = links_dict.keys()

    for doc_id in all_pages:
        contribution = contributions.get(doc_id, 0.0)
        new_ranks[doc_id] = base + dangling_share + alpha * contribution

    return new_ranks


def pagerank(links_dict, num_iterations=20, alpha=0.85):

    num_pages = len(links_dict)
    if num_pages == 0:
        return {}

    ranks = {doc_id: 1.0 / num_pages for doc_id in links_dict.keys()}

    for _ in range(num_iterations):
        contributions, dangling_mass = mapper(links_dict, ranks)
        ranks = reducer(links_dict, contributions, dangling_mass, num_pages, alpha=alpha)

    return ranks


def main():
    links_dict, id_to_url = load_links(DB_PATH)

    if not links_dict:
        print("Словарь связей пустой")
        return

    ranks = pagerank(links_dict, num_iterations=20, alpha=0.85)

    ranked_docs = sorted(ranks.items(), key=lambda x: x[1], reverse=True)

    print("PageRank для документов:")
    for doc_id, rank in ranked_docs:
        url = id_to_url.get(doc_id, "<unknown>")
        print(f"ID={doc_id:3d}  PR={rank:.6f}  URL={url}")


if __name__ == "__main__":
    main()
