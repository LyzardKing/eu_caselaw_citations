# -*- coding: utf-8 -*-
# %%
import json
import duckdb
from pprint import pprint

# %%
DB_FILE = "datasette/iuropa.duckdb"
connection = duckdb.connect(DB_FILE)

# %%
def fetch_query(id):
    command = f"""
with RECURSIVE paths(startNode, endNode, path) AS (
    SELECT -- define the path as the first edge of the traversal
        src AS startNode,
        dest AS endNode,
        [src, dest] AS path
    FROM edges
    WHERE startNode = '{id}'
    UNION ALL
    SELECT -- concatenate new edge to the path
        paths.startNode AS startNode,
        dest AS endNode,
        array_append(path, dest) AS path
    FROM paths
    JOIN edges ON paths.endNode = src
)
-- select the paths that are not subpaths of other paths
SELECT
    path
FROM paths
"""

    result = connection.execute(command).fetchall()
    return result

# %%
# fetch_query('ECLI:EU:C:1984:153_23')

# %%
def is_subset(list_1, list_2):
    return set(list_2).issubset(set(list_1))

# %%
def is_subsequence(list_1, list_2):
    # list_1 = list(map(str, list_1))
    # list_2 = list(map(str, list_2))
    return ''.join(list_2) in ''.join(list_1)

# %%
with open ('cases.json', 'r') as f:
    cases = json.load(f)

# %%
def clean_paths(case_list):
        for j in case_list:
            for k in case_list:
                if is_subsequence(j, k) and j != k:
                    case_list.remove(k)
                    break

def clean_file():
    for i in cases:
        clean_paths(i["english_paths"])
# %%
def save_cases(casedb, name='cases_new.json'):
    with open (name, 'w', encoding='utf8') as f:
        json.dump(casedb, f, indent=4, ensure_ascii=False)
# %%
print(cases[0]["english_paths"])

# %%
def merge_lists(l1, l2):
    # Merge two lists. If l2 is itself a list return multiple lists combining
    # l1 with all the lists in l2.
    # Always skip the first element of l2 (or its sublists), since it is the same as the last.
    if l2 == []:
        return [l1]
    if type(l2[0]) == list:
        return [l1 + l[1:] for l in l2]
    else:
        return [l1 + l2[1:]]
    
# %%
for i in range(len(cases)):
    cases[i]["full_paths"] = []
    for j in range(len(cases[i]["english_paths"])):
        old = cases[i]["english_paths"][j]
        new = [k[0] for k in fetch_query(cases[i]["english_paths"][j][-1])]
        merged = merge_lists(old, new)
        cases[i]["full_paths"] += merged
    # print(cases[i]["full_paths"])
    # print()
    for f in cases[i]["french_paths"]:
        if f not in cases[i]["full_paths"]:
            cases[i]["full_paths"].append(f)

# %%
save_cases()

# %%
with open ('cases_new.json', 'r') as f:
    cases_tmp = json.load(f)

for c in cases_tmp:
    del c["english_paths"]
    del c["french_paths"]

save_cases(cases_tmp, 'cases_full.json')


# %%
def list_text():
    text_list = {}
    with open ('cases_full.json', 'r') as f:
        cases = json.load(f)
    for c in cases:
        for p in c["full_paths"]:
            for i in p:
                if i not in text_list:
                    text_list[i] = get_text(i)
                # text_list[i] = get_text(i)
    return text_list

text_list = list_text()
# %%
def get_text(id):
    ecli = id.split('_')[0]
    paragraph = id.split('_')[1]
    command = f"""
SELECT
    text
    from paragraphs
    where ecli = '{ecli}' and paragraph_number = {paragraph}
"""
    result = connection.execute(command).fetchall()
    try:
        return result[0][0]
    except IndexError:
        return ""
# %%
save_cases(text_list, 'text_list.json')
