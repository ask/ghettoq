

class BaseBackend(object):

    def __init__(self, host=None, port=None, user=None, password=None,
            database=None, timeout=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.connection = None

    @property
    def client(self):
        if self.connection is None:
            self.connection = self.establish_connection()
        return self.connection
