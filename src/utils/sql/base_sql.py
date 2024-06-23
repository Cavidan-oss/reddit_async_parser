
class SQL():

    def read_sql(query, **params):

        if query.strip().endswith('.sql'):
            with open(query, 'r') as  file:
                query = file.read().strip()

        return query.format(**params)

