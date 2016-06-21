class Subject(object):
    def __init__(self):
        self.relations = []
        self.tables = []

    def __repr__(self):
        return '<Subject %s>' % id(self)
