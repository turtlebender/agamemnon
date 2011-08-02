from agamemnon.graph_constants import ASCII

class InMemoryDataStore(object):
    def __init__(self):
        self.tables = {}
        self.transaction = None

    def get_cf(self, cf_name):
        if not cf_name in self.tables:
            self.tables[cf_name] = self.create_cf(cf_name)
        return self.tables[cf_name]

    def create_cf(self, type, column_type=ASCII, super=False, index_columns=list()):
        self.tables[type] = ColumnFamily(type)
        return self.tables[type]

    def cf_exists(self, type):
        return type in self.tables.keys()

    def insert(self, cf, row, columns):

        def execute():
            cf.insert(row, columns)
        if self.transactions is not None:
            self.transactions.append(execute)
        else:
            execute()

    def remove(self, cf, row, columns=None, super_column=None):
        def execute():
            cf.remove(row, columns=columns, super_column=super_column)

        if self.transactions is not None:
            self.transactions.append(execute)
        else:
            execute()

    def start_batch(self):
        self.transactions = []

    def commit_batch(self):
        for transaction in self.transactions:
            transaction()
        self.transactions = None

class ColumnFamily(object):
    def __init__(self, name):
        self.data = {}
        self.name = name

    def get(self, row, columns=None, column_start=None, super_column=None, column_finish=None, column_count=100):
        try:
            if columns is None and column_start is None and super_column is None:
                return self.data[row]
            else:

                if super_column is None:
                    columns = self.data[row]
                else:
                    columns = self.data[row][super_column]
                results = {}
                for c in columns.keys():
                    if column_start is not None:
                        if c.startswith(column_start):
                            results[c] = columns[c]
                        continue
                    results[c] = columns[c]
            return results
        except KeyError:
            return {}

    def insert(self, row, columns, ttl=None):
        if not row in self.data:
            self.data[row] = {}
        if columns is not None:
            for c in columns.keys():
                self.data[row][c] = columns[c]

            #        if ttl is not None:
            #            def delete():
            #                for c in columns.keys():
            #                    del(self.data[row][c])
            #            Timer(ttl, delete, ()).start()

    def remove(self, row, columns=None, super_column=None):
        if columns is None and super_column is None:
            del(self.data[row])
        elif super_column is None:
            row = self.data[row]
            for c in columns:
                if c in row:
                    del(row[c])
        elif columns is None:
            row = self.data[row]
            del(row[super_column])
        else:
            sc = self.data[row][super_column]
            for c in columns:
                if c in sc:
                    del(rc[c])
    def get_indexed_slices(self, index_clause):
        expression = index_clause.expressions[0]
        column_name = expression.column_name
        value = expression.value
        for i in self.data.items():
            if i[1][column_name] == value:
                yield i[0], i[1]
  