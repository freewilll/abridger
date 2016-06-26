def generic_conn(request, database):
    result = database.connection

    def fin():
        result.close()
    request.addfinalizer(fin)

    return result
