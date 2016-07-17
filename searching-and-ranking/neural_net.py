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
