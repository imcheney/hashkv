#!/usr/bin/python3
# encoding:utf8

import sys
import io
import os

DB_FILE = 'db.data'

class DataBase(object):
    def __init__(self) -> None:
        super().__init__()
        self.data = {}
        self.f = open(DB_FILE, 'a+')
        self.build_index()

    def __del__(self) -> None:
        if self.f:
            self.f.close()

    def get(self, key):
        value = None
        index = None
        if key in self.data:
            index = self.data[key]
            print('index', index)
            value = self.read_file(index, key)
        print('\nDB RETURN', value, '\n')
        return value

    def set(self, key, value):
        index = self.write_file('set', key, value)
        self.data[key] = index
        print('file write index', index)
        print('\nDB RETURN', 'WRITE_OK', key, '\n')

    def remove(self, key):
        if key not in self.data:
            return None
        index = self.write_file('del', key, value)
        self.data[key] = index
        print('\nDB RETURN', 'REMOVE_OK', key, '\n')

    def write_file(self, cmd, key, value):
        if not self.f:
            return
        self.f.seek(0, io.SEEK_END)
        line = '{},{},{}\n'.format(cmd, key, value)
        self.f.write(line)
        self.f.flush()
        index = self.f.tell() - len(line)
        self.data[key] = index
        print('write', index, key, value)
        return index

    def read_file(self, index, key):
        if not self.f:
            return
        self.f.seek(index)
        line = self.f.readline()

        cmd, db_key, db_value = line.strip().split(',')
        if db_key != key:
            print('key_not_same_error', db_key, key)
            return None
        if cmd != 'set':
            return None
        print('value', db_value)
        return db_value

    def build_index(self):
        self.f.seek(0)
        last_offset = 0
        for line in self.f:
            cmd, db_key, db_value = line.split(',')
            self.data[db_key] = last_offset
            last_offset += len(line)
        print('index_built_succ', self.data)
    
    # TODO: doing compaction on same file may lose data if we suddenly have a power break
    def compact_log(self):
        self.f.seek(0)
        last_offset = 0
        temp_compact_data = {}
        print('compact_begin', os.path.getsize(DB_FILE))
        for line in self.f:
            cmd, db_key, db_value = line.strip().split(',')
            if cmd == 'set':
                temp_compact_data[db_key] = db_value
            if cmd == 'del' and db_key in temp_compact_data:
                del temp_compact_data[db_key]
            last_offset += len(line)
        print('compact_data_to_write', temp_compact_data)
        self.f.close()
        self.f = open(DB_FILE, 'w')  # change file open mode to truncate content
        self.f.truncate()
        for k, v in temp_compact_data.items():
            self.write_file('set', k, v)
        self.f.flush()
        self.f.close()
        self.f = open(DB_FILE, 'a+')    # reset to a+ mode
        print('compact_end', os.path.getsize(DB_FILE))
            


if __name__ == '__main__':
    print(r'''
    -----------------
        Hash KV
    -----------------
    ''')
    argc = len(sys.argv)
    func = ''
    key = ''
    value = ''
    if argc <= 1:
        print("commands: set key value, get key, remove key")
        exit
    if argc >= 2:
        func = sys.argv[1]
    if argc >= 3:
        key = sys.argv[2]
    if argc >= 4:
        value = sys.argv[3]
    db = DataBase()
    if func == 'get':
        db.get(key)
    elif func == 'set':
        db.set(key, value)
    elif func == 'remove':
        db.remove(key)
    elif func == 'compact':
        db.compact_log()
    else:
        print("commands: set key value, get key, remove key")