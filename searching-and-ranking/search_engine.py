import bs4
import re
import sqlite3
import urllib2
from urlparse import urljoin


ignore_words = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class Crawler(object):
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def dbcommit(self):
        self.conn.commit()

    def get_entry_id(self, table, field, value, create_new=True):
        curs = self.conn.execute("select rowid from %s where %s = ?" % (table, field), (value,))
        res = curs.fetchone()
        if res is None and create_new:
            curs = self.conn.execute("insert into %s (%s) values (?)" % (table, field), (value,))
            return curs.lastrowid
        else:
            return res[0]

    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return
        print 'Indexing %s' % url
        words = self.separate_words(self.get_text_only(soup))
        url_id = self.get_entry_id('url_list', 'url', url)
        for i in range(len(words)):
            word = words[i]
            if word in ignore_words:
                continue
            word_id = self.get_entry_id('word_list', 'word', word)
            self.conn.execute('insert into word_location (url_id, word_id, location) \
                               values (?, ?, ?)', (url_id, word_id, i))

    def get_text_only(self, soup):
        v = soup.string
        if not v:
            c = soup.contents
            result_text = ''
            for t in c:
                subtext = self.get_text_only(t)
                result_text += subtext + '\n'
            return result_text
        return v.strip()

    def separate_words(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s]

    def is_indexed(self, url):
        u = self.conn.execute("select rowid from url_list where url = ?", (url,)).fetchone()
        if u:
            v = self.conn.execute("select * from word_location where url_id=?", (u[0],)).fetchone()
            if v:
                return True
        return False

    def add_link_ref(self, url_from, url_to, link_text):
        words = self.separate_words(link_text)
        from_id = self.get_entry_id('url_list', 'url', url_from)
        to_id = self.get_entry_id('url_list', 'url', url_to)
        if from_id == to_id:
            return
        curs = self.conn.execute("insert into link (from_id, to_id) values (?, ?)",
                                 (from_id, to_id))
        link_id = curs.lastrowid
        for word in words:
            if word in ignore_words:
                continue
            word_id = self.get_entry_id('word_list', 'word', word)
            self.conn.execute("insert into link_words (link_id, word_id) values (?, ?)",
                              (link_id, word_id))

    def crawl(self, pages, depth=2):
        for i in range(depth):
            new_pages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                soup = bs4.BeautifulSoup(c.read(), "html.parser")
                self.add_to_index(page, soup)

                links = soup('a')
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.is_indexed(url):
                            new_pages.add(url)
                        link_text = self.get_text_only(link)
                        self.add_link_ref(page, url, link_text)
                self.dbcommit()
            pages = new_pages

    def create_index_tables(self):
        self.conn.execute('create table url_list(url)')
        self.conn.execute('create table word_list(word)')
        self.conn.execute('create table word_location(url_id, word_id, location)')
        self.conn.execute('create table link(from_id integer, to_id integer)')
        self.conn.execute('create table link_words(word_id, link_id)')
        self.conn.execute('create index word_idx on word_list(word)')
        self.conn.execute('create index url_idx on url_list(url)')
        self.conn.execute('create index word_url_idx on word_location(word_id)')
        self.conn.execute('create index url_to_idx on link(to_id)')
        self.conn.execute('create index url_from_idx on link(from_id)')
        self.dbcommit()

    def calculate_pagerank(self, iterations=20):
        self.conn.execute('drop table if exists pagerank')
        self.conn.execute('create table pagerank(url_id primary key, score)')

        # initialize every url with a pagerank of 1
        self.conn.execute('insert into pagerank select rowid, 1.0 from url_list')
        self.dbcommit()

        for i in range(iterations):
            print "Iteration %d" % (i)
            for (url_id,) in self.conn.execute('select rowid from url_list'):
                pagerank = 0.15    # minimum value
                linkers = self.conn.execute('select distinct from_id from link where to_id=?',
                                            (url_id,))
                for (linker_id,) in linkers:
                    linker_pagerank = self.conn.execute(
                        'select score from pagerank where url_id=?', (linker_id,)).fetchone()[0]
                    linker_num_links = self.conn.execute(
                        'select count(*) from link where from_id=?', (linker_id,)).fetchone()[0]
                    pagerank += 0.85 * (linker_pagerank / linker_num_links)
                    self.conn.execute('update pagerank set score=? where url_id=?',
                                      (pagerank, url_id))
                self.dbcommit()


class Searcher(object):
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def get_match_rows(self, query):
        fields = 'w0.url_id'
        tables = ''
        where_clauses = ''
        word_ids = []
        table_num = 0

        for word in [w.lower() for w in query.split(' ') if w]:
            curs = self.conn.execute("select rowid from word_list where word=?", (word,)).fetchone()
            if curs:
                word_id = curs[0]
                word_ids.append(word_id)
                if table_num > 0:
                    tables += ','
                    where_clauses += ' and w%d.url_id=w%d.url_id and ' % (table_num - 1, table_num)
                fields += ',w%d.location' % table_num
                tables += 'word_location w%d' % table_num
                where_clauses += 'w%d.word_id=%d' % (table_num, word_id)
                table_num += 1

        if not tables:
            return

        full_query = "select %s from %s where %s" % (fields, tables, where_clauses)
        curs = self.conn.execute(full_query)
        rows = [row for row in curs if curs is not None]

        return rows, word_ids

    def frequency_score(self, rows):
        counts = dict((row[0], 0) for row in rows)
        for row in rows:
            counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        locations = dict((row[0], 1000000) for row in rows)
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc

        return self.normalize_scores(locations, small_is_better=True)

    def distance_score(self, rows):
        # If there's only one word, everyone wins!
        if len(rows[0]) <= 2:
            return dict((row[0], 1.0) for row in rows)

        min_distances = dict((row[0], 1000000) for row in rows)
        for row in rows:
            dist = sum(abs(row[i] - row[i-1]) for i in range(2, len(row)))
            if dist < min_distances[row[0]]:
                min_distances[row[0]] = dist

        return self.normalize_scores(min_distances, small_is_better=True)

    def inbound_link_score(self, rows):
        unique_url_ids = set(row[0] for row in rows)
        inbound_counts = dict((u, self.conn.execute('select count(*) from link where to_id=?',
                                                    (u,)).fetchone()[0])
                              for u in unique_url_ids)
        return self.normalize_scores(inbound_counts)

    def pagerank_score(self, rows):
        pageranks = dict((row[0], self.conn.execute('select score from pagerank where url_id=?',
                                                    (row[0],)).fetchone()[0])
                         for row in rows)
        return self.normalize_scores(pageranks)

    def link_text_score(self, rows, word_ids):
        link_scores = dict((row[0], 0) for row in rows)
        q = """select link.from_id, link.to_id from link_words, link where word_id=?
               and link_words.link_id=link.rowid"""
        for word_id in word_ids:
            for (from_id, to_id) in self.conn.execute(q, (word_id,)):
                if to_id in link_scores:
                    pr = self.conn.execute('select score from pagerank where url_id=?',
                                           (from_id,)).fetchone()[0]
                    link_scores[to_id] += pr
        return self.normalize_scores(link_scores)

    def normalize_scores(self, scores, small_is_better=False):
        vsmall = 0.00001    # Avoid division by zero errors
        if small_is_better:
            min_score = min(scores.values())
            return dict((u, float(min_score) / max(vsmall, l)) for u, l in scores.iteritems())
        else:
            max_score = max(scores.values() + [vsmall])
            return dict((u, float(c) / max_score) for u, c in scores.iteritems())

    def get_scored_list(self, rows, word_ids):
        total_scores = dict((row[0], 0) for row in rows)
        weights = [(0.3, self.frequency_score(rows)), (0.2, self.location_score(rows)),
                   (0.2, self.distance_score(rows)), (0.2, self.pagerank_score(rows)),
                   (0.1, self.link_text_score(rows, word_ids))]

        for weight, scores in weights:
            for url in total_scores:
                total_scores[url] += weight * scores[url]

        return total_scores

    def get_url_name(self, id_):
        return self.conn.execute('select url from url_list where rowid = ?', (id_,)).fetchone()[0]

    def query(self, q):
        if not q.strip():
            print 'Empty query'
            return

        query_result = self.get_match_rows(q)
        if not query_result:
            print 'No results found'
            return

        rows, word_ids = query_result
        scores = self.get_scored_list(rows, word_ids)
        ranked_scores = sorted([(score, url) for url, score in scores.items()], reverse=1)
        for score, url_id in ranked_scores[0:10]:
            print '%f\t%s' % (score, self.get_url_name(url_id))
