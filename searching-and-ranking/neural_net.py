from math import tanh

import sqlite3


table_names = ['word_hidden', 'hidden_url']


class SearchNet(object):
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def make_tables(self):
        self.conn.execute('create table hidden_node(create_key)')
        self.conn.execute('create table word_hidden(from_id, to_id, strength)')
        self.conn.execute('create table hidden_url(from_id, to_id, strength)')
        self.conn.commit()

    def get_strength(self, from_id, to_id, layer):
        table = table_names[layer]
        res = self.conn.execute('select rowid, strength from %s where from_id=? and to_id=?' % table,
                                (from_id, to_id)).fetchone()
        if res is None:
            if layer == 0:
                return None, -0.2
            if layer == 1:
                return None, 0
        return res[0]

    def set_strength(self, from_id, to_id, layer, strength):
        rowid, _ = self.get_strength(from_id, to_id, layer)
        table = table_names[layer]
        if rowid is None:
            self.conn.execute('insert into %s (from_id, to_id, strength) values (?, ?, ?)' % table,
                              (from_id, to_id, strength))
        else:
            self.conn.execute('update %s set strength=? where rowid=?' % table, (strength, rowid))

    def generate_hidden_node(self, word_ids, url_ids):
        if len(word_ids) > 3:
            return None
        # check if we already created a node for this set of words
        create_key = '_'.join(sorted([str(wi) for wi in word_ids]))
        res = self.conn.execute('select rowid from hidden_node where create_key=?',
                                (create_key,)).fetchone()
        if res is None:
            curs = self.conn.execute('insert into hidden_node (create_key) values (?)',
                                     (create_key,))
            hidden_id = curs.lastrowid
            # put in some default weights
            for word_id in word_ids:
                self.set_strength(word_id, hidden_id, 0, 1.0 / len(word_ids))
            for url_id in url_ids:
                self.set_strength(hidden_id, url_id, 1, 0.1)
            self.conn.commit()

    def get_all_hidden_ids(self):
        hidden_ids = set()
        for word_id in self.word_ids:
            curs = self.conn.execute('select to_id from word_hidden where from_id=?', (word_id,))
            hidden_ids.update(row[0] for row in curs)
        for url_id in self.url_ids:
            curs = self.conn.execute('select from_id from hidden_url where to_id=?', (url_id,))
            hidden_ids.update(row[0] for row in curs)
        return hidden_ids

    def set_up_network(self, word_ids, url_ids):
        self.word_ids = word_ids
        self.url_ids = url_ids
        self.hidden_ids = self.get_all_hidden_ids()

        # node outputs
        self.ai = [1.0] * len(self.word_ids)
        self.ah = [1.0] * len(self.hidden_ids)
        self.ao = [1.0] * len(self.url_ids)

        # create weights matrix
        self.wi = [[self.get_strength(word_id, hidden_id, 0) for hidden_id in self.hidden_ids]
                   for word_id in self.word_ids]
        self.wo = [[self.get_strength(hidden_id, url_id, 1) for url_id in self.url_ids]
                   for hidden_id in self.hidden_ids]

    def feed_forward(self):
        # the only inputs are the query words
        for i in range(len(self.word_ids)):
            self.ai[i] = 1.0

        # hidden activations
        for j in range(len(self.hidden_ids)):
            total = 0.0
            for i in range(len(self.word_ids)):
                total += self.ai[i] * self.wi[i][j]
            self.ah[j] = tanh(total)

        # output activations
        for k in range(len(self.url_ids)):
            total = 0.0
            for j in range(len(self.hidden_ids)):
                total += self.ah[j] * self.wo[j][k]

        return self.ao[:]

    def get_result(self, word_ids, url_ids):
        self.set_up_network(word_ids, url_ids)
        return self.feed_forward()
