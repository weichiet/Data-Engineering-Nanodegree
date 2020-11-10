class CheckNull:

    is_null_sql_template = """
        SELECT COUNT(*)
        FROM {table}
        WHERE {column} IS NULL;
    """

    def get_check_null_test_cases(columns_dict):
        '''
        Generate sql statements to checks if certain column contains NULL values
        '''
        sql_statements = []
        expected_values = []

        for table in columns_dict.keys():
            for i in range(len(columns_dict[table])):
                is_null_sql = CheckNull.is_null_sql_template.format(table = table, column = columns_dict[table][i])
                sql_statements.append(is_null_sql)
                expected_values.append(0)

        return sql_statements, expected_values