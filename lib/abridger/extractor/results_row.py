class ResultsRow(object):
    def __init__(self, table, row, subjects=None, sticky=False, count=1):
        if subjects is None:
            subjects = set()
        self.table = table
        self.row = row
        self.subjects = subjects
        self.sticky = sticky
        self.count = 1

    def __str__(self):
        return 'row=%s subjects=%s sticky=%s' % (
            self.row, self.subjects, self.sticky)

    def __repr__(self):
        return '<ResultsRow %s>' % str(self)

    def __lt__(self, other):
        return self.row < other.row

    def merge(self, other):
        '''
            Merge two results rows. not-null values take precedence over nulls
        '''

        changed_row = None
        for i, (self_val, other_val) in enumerate(zip(self.row, other.row)):
            if (self_val is None) and (other_val is not None):
                if not changed_row:
                    changed_row = list(self.row)
                changed_row[i] = other_val

        if changed_row is not None:
            self.row = tuple(changed_row)
