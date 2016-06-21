class NotNullColumn(object):
    def __init__(self, table, column, foreign_key):
        self.table = table
        self.column = column
        self.foreign_key = foreign_key
