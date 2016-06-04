def generic_conn(request, dbconn):
    result = dbconn.connection

    def fin():
        result.close()
    request.addfinalizer(fin)

    return result
