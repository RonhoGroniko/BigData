import sqlite3
from pregel import Pregel, Vertex

DB_PATH = "search.db"
ALPHA = 0.85
MAX_SUPERSTEPS = 20


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


class PageRankVertex(Vertex):
    def __init__(self, doc_id, out_vertices, num_vertices, alpha=ALPHA, all_vertices=None):
        initial_value = 1.0 / num_vertices if num_vertices > 0 else 0.0
        super(PageRankVertex, self).__init__(doc_id, initial_value, out_vertices)
        self.num_vertices = num_vertices
        self.damping = alpha
        self.all_vertices = all_vertices

    def update(self):

        if self.superstep >= MAX_SUPERSTEPS:
            self.active = False
            self.outgoing_messages = []
            return

        self.outgoing_messages = []

        if self.superstep == 0:
            self._send_rank()
            return

        incoming_sum = 0.0
        for doc_from, value in self.incoming_messages:
            incoming_sum += value

        base = (1.0 - self.damping) / self.num_vertices if self.num_vertices > 0 else 0.0
        new_rank = base + self.damping * incoming_sum

        self.value = new_rank

        self._send_rank()

    def _send_rank(self):

        if self.num_vertices == 0:
            return

        if self.out_vertices:
            share = self.value / float(len(self.out_vertices))
            for neighbour in self.out_vertices:
                self.outgoing_messages.append((neighbour, share))
        else:
            if self.all_vertices:
                share = self.value / float(self.num_vertices)
                for page in self.all_vertices:
                    self.outgoing_messages.append((page, share))


def build_vertices(links_dict):

    num_vertices = len(links_dict)
    vertices_by_id = {}
    vertices = []

    for doc_id in links_dict.keys():
        vertex = PageRankVertex(doc_id, [], num_vertices)
        vertices_by_id[doc_id] = vertex
        vertices.append(vertex)

    for vertex in vertices:
        vertex.all_vertices = vertices

    for doc_id, neighbors_ids in links_dict.items():
        vertex = vertices_by_id[doc_id]
        vertex.out_vertices = [vertices_by_id[n_id] for n_id in neighbors_ids]

    return vertices


def run_pagerank_pregel():
    links_dict, id_to_url = load_links(DB_PATH)

    if not links_dict:
        print("Словарь связей пустой")
        return

    vertices = build_vertices(links_dict)

    num_workers = 4
    job = Pregel(vertices, num_workers=num_workers)
    job.run()

    ranks = {}
    for vertex in vertices:
        ranks[vertex.id] = vertex.value

    ranked_docs = sorted(ranks.items(), key=lambda x: x[1], reverse=True)

    print("PageRank (Pregel) для документов:")
    for doc_id, rank in ranked_docs:
        url = id_to_url.get(doc_id, "<unknown>")
        print("ID={:3d}  PR={:.6f}  URL={}".format(doc_id, rank, url))


def main():
    run_pagerank_pregel()


if __name__ == "__main__":
    main()
