from sqlalchemy.dialects.postgresql import insert


class PostgresUpsertFactory:
    def build(self, constraint):
        def postgres_upsert(table, conn, keys, dataframe_values):
            """
            Example
            -------
            test = pd.DataFrame({"name": ["Teste"]})

            upsert_method_factory = PostgresUpsertFactory()
            upsert_method = upsert_method_factory.build('roles_unique_name')
            df.to_sql('roles',
                engine,schema='public',
                if_exists='append',
                index=False,
                method=upsert_method)

            See
            ----
            https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
            https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
            """
            data = [dict(zip(keys, row)) for row in dataframe_values]

            insert_statement = insert(table.table).values(data)
            upsert_statement = insert_statement.on_conflict_do_update(
                constraint=constraint,
                set_={c.key: c for c in insert_statement.excluded},
            )

            conn.execute(upsert_statement)

        return postgres_upsert
