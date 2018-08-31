from Common.DB.SQLServer_Client import SQLServerClient


class sample:
    def __init__(self):
        pass

    def sample(self):
        self.sql_cli = SQLServerClient()
        aa = self.sql_cli.fetch_one()

