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
            if curs is not None:
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
        rows = [row for row in curs]

        return rows, word_ids
