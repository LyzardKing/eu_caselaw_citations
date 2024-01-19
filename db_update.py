import duckdb
import eurlex

DB_FILE = "par_to_par.db"

def _get_ecli(celex: str) -> str:
    ecli = eurlex.case_info(celex)[0]["ecli"]
    if ecli is None:
        return ""
    return ecli

con = duckdb.connect(DB_FILE)
con.create_function("get_ecli", _get_ecli)

def _add_ecli_cols():
    con.sql(
        """
        alter table citations
        add column ECLI_FROM varchar;
        """)
    con.sql(
        """
        alter table citations
        add column ECLI_TO varchar;
    """
    )

def _set_ecli():
        command = """
update citations 
set ECLI_FROM = get_ecli(CELEX_FROM)
"""
        con.sql(command)
        command = """
update citations 
set ECLI_TO = get_ecli(CELEX_TO)
"""
        con.sql(command)

# _add_ecli_cols()
_set_ecli()