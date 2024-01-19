from datasette import hookimpl
from rouge import Rouge 

def rouge_sql(hypothesis, reference):
    rouge = Rouge()
    return rouge.get_scores(hypothesis, reference)[0]["rouge-l"]["f"]

@hookimpl
def prepare_connection(conn):
    conn.create_function("rouge_score", 2, rouge_sql)