class NotNullColumn(object):
    def __init__(self, table, col, foreign_key):
        self.table = table
        self.col = col
        self.foreign_key = foreign_key
