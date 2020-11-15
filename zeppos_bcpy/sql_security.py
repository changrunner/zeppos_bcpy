class SqlSecurity:
    @staticmethod
    def use_integrated_security(username, password):
        return not (username and password)
