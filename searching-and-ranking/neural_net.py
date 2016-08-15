from math import tanh

import sqlite3


table_names = ['word_hidden', 'hidden_url']


def dtanh(y):
    return 1.0 - y * y


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
        res = self.conn.execute('select strength from %s where from_id=? and to_id=?' % table,
                                (from_id, to_id)).fetchone()
        if res is None:
            if layer == 0:
                return -0.2
            if layer == 1:
                return 0
        return res[0]

    def set_strength(self, from_id, to_id, layer, strength):
        table = table_names[layer]
        res = self.conn.execute('select rowid from %s where from_id=? and to_id=?' % table,
                                (from_id, to_id)).fetchone()
        if res is None:
            self.conn.execute('insert into %s (from_id, to_id, strength) values (?, ?, ?)' % table,
                              (from_id, to_id, strength))
        else:
            self.conn.execute('update %s set strength=? where rowid=?' % table, (strength, res[0]))

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
        return list(hidden_ids)

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
            self.ao[k] = tanh(total)

        return self.ao[:]

    def get_result(self, word_ids, url_ids):
        self.set_up_network(word_ids, url_ids)
        return self.feed_forward()

    def back_propagate(self, targets, n=0.5):
        # calculate errors for output
        output_deltas = [0.0] * len(self.url_ids)
        for k in range(len(self.url_ids)):
            error = targets[k] - self.ao[k]
            output_deltas[k] = dtanh(self.ao[k]) * error

        # calculate errors for hidden layer
        hidden_deltas = [0.0] * len(self.hidden_ids)
        for j in range(len(self.hidden_ids)):
            error = 0.0
            for k in range(len(self.url_ids)):
                error = error + output_deltas[k] * self.wo[j][k]
            hidden_deltas[j] = dtanh(self.ah[j]) * error

        # update output weights
        for j in range(len(self.hidden_ids)):
            for k in range(len(self.url_ids)):
                change = output_deltas[k] * self.ah[j]
                self.wo[j][k] = self.wo[j][k] + n * change

        # update input weights
        for i in range(len(self.word_ids)):
            for j in range(len(self.hidden_ids)):
                change = hidden_deltas[j] * self.ai[i]
                self.wi[i][j] = self.wi[i][j] + n * change

    def train_query(self, word_ids, url_ids, selected_url):
        # generate a hidden node if necessary
        self.generate_hidden_node(word_ids, url_ids)

        self.set_up_network(word_ids, url_ids)
        self.feed_forward()
        targets = [0.0] * len(url_ids)
        targets[url_ids.index(selected_url)] = 1.0
        self.back_propagate(targets)
        self.update_database()

    def update_database(self):
        for i in range(len(self.word_ids)):
            for j in range(len(self.hidden_ids)):
                self.set_strength(self.word_ids[i], self.hidden_ids[j], 0, self.wi[i][j])
        for j in range(len(self.hidden_ids)):
            for k in range(len(self.url_ids)):
                self.set_strength(self.hidden_ids[j], self.url_ids[k], 1, self.wo[j][k])
        self.conn.commit()
