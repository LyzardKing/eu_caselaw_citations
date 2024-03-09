import duckdb
import os
import eurlex
import re
import logging
import pandas as pd
import networkx as nx

logging.basicConfig(filename="example.log", encoding="utf-8", level=logging.DEBUG)

# from sentence_transformers import SentenceTransformer

# model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
BATCH_SIZE = 256
FILENAME = "iuropa_text.gz.parquet"
DB_FILE = "datasette/iuropa.duckdb"
REGEX_ART = r"\d+(?:[\w\s,]+?\d+)*"
# REGEX_ART = r"(\d+)(?:([\w\s]+?)(\d+))*"
# REGEX = fr"(?:C.|\()(\d+?\/\d+)(?:.{{0,30}}? points? (({REGEX_ART})))"
REGEX = rf"(((?:C.|\()(?:\d+?\/\d+),\s)|(EU:\w:\d{{4}}:\d+))(?:.{{0,40}}?points? ({REGEX_ART}))"
# (((?:C.|\()(?:\d+?\/\d+),\s)|(EU:\w:\d{4}:\d+))(?:.{0,30}?points? (\d+(?:[\w\s,]+?\d+)*))
# (((?:C.|\()(?:\d+?\/\d+),\s)|(EU:\w:\d{4}:\d+))(?:.{0,30}?points? (\d+(?:[\w\s,]+?\d+)*))
# (?:(arr.t.|affaire){0,40}?[^\/]points?\s(\d+(?:[\w\s,]+?\d+)*?)([^\/\d)T.]{0,30})(\d+\/\d+))|(?:([^T]\D)(\d+\/\d+)).{0,30}points?\s(\d+(?:[\w\s,]+?\d+)*?)|(\S+)(?:,\se\.a\.)?(?:,\spr.cit.)?,\spoints?\s(\d+(?:[\w\s,]+?\d+)*?)

# arr.t.{0,40}?[^\/]points?\s(\d+(?:[\w\s,]+?\d+)*)[^\/\d]+(\d+\/\d+)(?<!\/)


class IuropaDB:
    def __init__(self, court="Court of Justice", exclude_par_zero=True):
        f"""Import the database from the parquet file {FILENAME} into the db {DB_FILE}

        Args:
            court (str, optional): _description_. Defaults to 'Court of Justice'.
            exclude_par_zero (bool, optional): _description_. Defaults to True.
        """
        if not os.path.exists(DB_FILE):
            self.con = duckdb.connect(DB_FILE)
            self.con.sql(
                f"""
            create table lines as
            select * from read_parquet({FILENAME})
            """
            )
            self._init_paragraphs(court, exclude_par_zero)
            self._add_citations_col()
            self._populate_citations_col()
        self.con = duckdb.connect(DB_FILE)
        self.con.create_function("get_ecli", self._get_ecli)
        self.con.create_function("get_par_numbers", self._get_par_numbers)

    def __enter__(self):
        self.con = duckdb.connect(DB_FILE)
        # self.con.create_function("get_ecli", self._get_ecli)
        # self._add_citations_col()
        # self._add_embeddings_col()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # print("Closing db")
        self.con.close()

    def _init_paragraphs(self, court, exclude_par_zero):
        # conditions = [court, exclude_par_zero]s
        self.con.sql(
            f"""
            create table paragraphs as
            select ecli, paragraph_number, any_value(language), string_agg(text, ' ') as text from lines
            where paragraph_number != 0 and court = 'Court of Justice' and language = 'FR'
            group by ecli, paragraph_number
            order by ecli, paragraph_number, ANY_VALUE(line_id);
        """
        )

    def _add_embeddings_col(self):
        self.con.sql(
            """
            alter table paragraphs
            add column embedding varchar;
        """
        )

    def _add_citations_col(self):
        self.con.sql(
            """
            alter table paragraphs
            add column citations varchar[];
        """
        )

    def _populate_citations_col(self):
        self.con.sql(
            f"""
            update paragraphs
            set citations = regexp_extract_all(text, '{REGEX}');
        """
        )

    def _get_stats(self):
        citation_count = self.con.sql(
            "select sum(len(citations)) from paragraphs where citations != [];"
        ).fetchone()
        paragraph_n = self.con.sql("select count(*) from paragraphs;").fetchone()
        paragraph_cit_n = self.con.sql(
            "select count(*) from paragraphs where citations != [];"
        ).fetchone()

        return citation_count, paragraph_n, paragraph_cit_n

    def _get_ecli(self, case: str) -> str:
        result = eurlex.search(case)[0]
        return result

    def _get_par_numbers(self, x: str) -> duckdb.type("int32[]"):
        tmp = re.search(REGEX_ART, x)
        result = []
        if not tmp:
            return result
        i = re.finditer(r"(\d+)\sà\s(\d+)", x)
        for p in i:
            result += [*range(int(p.group(1)), int(p.group(2)) + 1)]
            x = x.replace(p.group(0), "")
        result += [int(x) for x in re.findall("\d+", x)]
        # print(result)
        return result

        # if not i.group(2):
        #     result = [int(x)]
        # elif "et" in i.group(2) or "ainsi que" in i.group(2) or "avec" in i.group(2) or "and" in i.group(2):
        #     logging.debug(f"AND: {i.group(2)}: {x} -> {i}")
        #     result = [int(i.group(1))]
        #     result += [int(j) for j in re.findall("\d+", i.group(2))]
        #     result.append(int(i.group(3)))
        # elif "à" in i.group(2) or "to" in i.group(2):
        #     logging.debug("TO: " + i.group(2))
        #     result = [*range(int(i.group(1)), int(i.group(3))+1, 1)]
        # else:
        #     # Try to read the first paragraph if all else fails
        #     logging.debug("MISSED: " + i.group(2))
        #     result = [int(i.group(1))]
        # return result

    def print_stats(self):
        """Print the stats on citations in the db."""
        citation_count, paragraph_n, paragraph_cit_n = self._get_stats()

        print(
            f"The database contains:\n - {paragraph_n[0]:_} paragraphs, of which",
            f"\n - {paragraph_cit_n[0]:_} contain citations, for a total of",
            f"\n - {citation_count[0]:_} citations*",
            "\n\n* citations such as 'a and b', 'a to b' are considered as one",
        )

    def set_citations_table(
        self,
        ecli,
        paragraph=None,
        unnest=False,
        text=False,
        show_empty=False,
        to_dict=False,
        save=False,
    ):
        """Extract the citations from the db

        Args:
            ecli (str): ECLI of the case from which to extract the citations. Set to None to run for all.
            paragraph (str, optional): Paragraph number from which to extract the citations. Defaults to None to run for all..
            unnest (bool, optional): Split the citations in one line for each source. Defaults to False.
            text (bool, optional): Fetch the text for the cited and citing paragraphs. Defaults to False.
            show_empty (bool, optional): Include paragraphs with no further citations. Defaults to False.
            to_dict (bool, optional): Return the result as a python dict. Defaults to False.
            save (bool, optional): Save the result to a new table in the db. Defaults to False.
        """
        ECLI = f"ecli = '{ecli}'"
        PARAGRAPH = f"paragraph_number = '{paragraph}'"
        CITATIONS = "citations != []"
        # LANG = "language='FR'"
        conditions = [ECLI, PARAGRAPH, CITATIONS]
        command = f"""
            select * from paragraphs
            CONDITIONS
        """
        if unnest:
            # get_ecli(concat('ECLI:', regexp_extract(citation, '(EU:C:\d{4}:\d+)', 1)), regexp_extract(citation, '(?:C.|\()(\d+?\/\d+))', 1)) 
            command = """
with tmp_table as (
    select  ecli, 
            paragraph_number,
            text,
            unnest(citations) as citation,
            regexp_extract(citation, '(EU:C:\d+:\d+)', 1) as cited_ecli,
            regexp_extract(citation, '(?:C.|\()(\d+?\/\d+)', 1) as cited_caseno,
            get_par_numbers(split_part(regexp_extract(citation, 'points? (\d+(?:[\w\s,]+?\d+)*)', 1), 'du', 1)) as cited_paragraph
    from paragraphs
    CONDITIONS
)
select tmp_table.ecli, tmp_table.paragraph_number, tmp_table.cited_ecli, tmp_table.cited_caseno, tmp_table.cited_paragraph
from tmp_table"""
        if text:
            command = command.replace(
                "tmp_table.paragraph_number,",
                "tmp_table.paragraph_number, tmp_table.text,",
            )
            command = command.replace(
                "tmp_table.cited_paragraph",
                "tmp_table.cited_paragraph, destination.cited_text",
            )
            command = command.replace(
                "select tmp_table.ecli",
                """, destination as (
                    select string_agg(text, ' ') as cited_text
                    from paragraphs
                    where ecli = cited_ecli
                    and list_contains(cited_paragraph, paragraph_number)
                )
                select tmp_table.ecli""",
            )
            command += ", destination"
        if show_empty:
            conditions.remove(CITATIONS)
        if not paragraph:
            conditions.remove(PARAGRAPH)
        if not ecli:
            conditions.remove(ECLI)
        if len(conditions) > 0:
            command = command.replace("CONDITIONS", "where " + " and ".join(conditions))
        print(command)
        if save:
            command = "create table citations as " + command
            self.con.sql(command)
            self.update_ecli()
            return
        if to_dict:
            return self.con.sql(command).fetchall()
        else:
            self.con.sql(command).show()

    def get_citations(self, a=(None, None), b=(None, None)):
        """Get the citation between two cases/paragraphs.

        Args:
            a (tuple, optional): Citing case and paragraph. Defaults to (None, None).
            b (tuple, optional): Cited case and paragraph. Defaults to (None, None).
        """
        command = "select * from citations"

        if a[0] is not None:
            command += f" where ecli='{a[0]}'"
            if a[1] is not None:
                command += f" and paragraph_number='{a[1]}'"

        if b[0] is not None:
            command += f" where cited_ecli='{b[0]}'"
            if b[1] is not None:
                command += f" and list_contains(cited_paragraph, '{b[1]}')"

        self.con.sql(command).show()

    def update_ecli(self):
        command = """
update citations 
set cited_ecli = get_ecli(cited_caseno)
where cited_ecli = ''
"""
        self.con.sql(command)
        command = """
update citations 
set cited_ecli = concat('ECLI:', cited_ecli)
where starts_with(cited_ecli, 'EU:C:')
"""
        self.con.sql(command)

    def get_graph(self, ecli, par, save=False):
        """Generate the graph starting from a particular case.
        In the future from multiple cases.

        Args:
            ecli (str): ECLI of the case from which to generate the graph.
            paragraph (str, optional): Paragraph number from which to generate the graph. Defaults to None to run for all..
            save (bool, optional): Export the graph to a graphml file. Defaults to False.
        """
        base = f"""
with recursive subgraph as (
select ecli, paragraph_number, cited_ecli, cited_paragraph_number from (
select ecli, paragraph_number, cited_ecli, unnest(cited_paragraph) as cited_paragraph_number from citations 
where cited_ecli = '{ecli}'
)
where cited_paragraph_number = {par}
UNION
select s1.ecli as ecli, s1.paragraph_number as paragraph_number, s1.cited_ecli as cited_ecli, s1.cited_paragraph_number as cited_paragraph_number from (
select citations.ecli as ecli, citations.paragraph_number as paragraph_number, citations.cited_ecli as cited_ecli, unnest(citations.cited_paragraph) as cited_paragraph_number
from subgraph, citations
where citations.cited_ecli=subgraph.ecli
) as s1, subgraph
where s1.cited_paragraph_number=subgraph.paragraph_number
)
"""
        command = (
            base
            + """ select distinct concat(ecli, '_', paragraph_number) as citing, concat(cited_ecli, '_', cited_paragraph_number) as cited from subgraph"""
        )
        if save:
            nodes = (
                base
                + f"""
select distinct concat(ecli, '_', paragraph_number) as par_id, ecli, paragraph_number from subgraph
union distinct
select DISTINCT  concat(cited_ecli, '_', cited_paragraph_number) as par_id, cited_ecli as ecli, cited_paragraph_number as paragraph_number from subgraph
"""
            )
            p_graph = self.con.query(command).to_df()
            node_data = self.con.query(nodes).to_df().set_index("par_id")
            graph = nx.from_pandas_edgelist(
                p_graph, "citing", "cited", create_using=nx.DiGraph()
            )
            nx.set_node_attributes(graph, ":Paragraph", "labels")
            nx.set_edge_attributes(graph, ":REFERS_TO", "label")
            nx.set_node_attributes(graph, node_data.to_dict("index"))
            for i, n in list(graph.nodes(data=True)):
                if n == {}:
                    continue
                if n["labels"] == ":Paragraph":
                    a, _ = i.split("_")
                    if not graph.has_node(a):
                        graph.add_node(a, labels=":Case", ecli=a)
                    graph.add_edge(i, a, label=":BELONGS_TO")
            nx.write_graphml(graph, f"{ecli}.graphml", named_key_ids=True)
            return graph
            # self.get_all_paths(graph)
        else:
            self.con.sql(command).show()

    def get_nodes_with_attribute(self, graph, attribute, value):
        """Get all nodes in the graph with a certain attribute.
        Example: get all Paragraph nodes.

        Args:
            graph (graph): Variable containing the networkx graph.
            attribute (str): Specific attribute we are interested in.
            value (str): Value of the attribute

        Returns:
            list: List of items that have the attribute.
        """
        selected_data = []
        for n, d in graph.nodes(data=True):
            if attribute in d and d[attribute] == value:
                selected_data.append(n)
        return selected_data

    def get_all_paths(self, G):
        """Get all paths from roots to leaves.

        Args:
            G (graph): Graph from which to extract the paths.
        """
        roots = []
        leaves = []
        for node in self.get_nodes_with_attribute(G, "labels", ":Paragraph"):
            if G.in_degree(node) == 0:  # it's a root
                roots.append(node)
            elif G.out_degree(node) == 0:  # it's a leaf
                leaves.append(node)

        for root in roots:
            for leaf in leaves:
                for path in nx.all_simple_paths(G, root, leaf):
                    print(path)


if __name__ == "__main__":
    with IuropaDB() as iuropadb:
        iuropadb.print_stats()
        # iuropadb.get_citations("ECLI:EU:C:2021:201", unnest=True)
