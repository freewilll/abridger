def generic_conn(request, dbconn):
    result = dbconn.connect()

    def fin():
        result.close()
    request.addfinalizer(fin)

    return result
