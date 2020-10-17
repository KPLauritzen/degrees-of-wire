import itertools
from typing import List

import networkx as nx
from kedro.pipeline import Pipeline, node


def create_graph(lines: List[dict]) -> nx.Graph:
    G = nx.Graph()
    for line in lines:
        if len(line["cast"]) < 2:
            continue
        else:
            connections = itertools.combinations(line["cast"], 2)
            for con in connections:
                G.add_edge(*con, work=line["title"])
    return G


def create_network_pipeline():
    return Pipeline([
        node(
            create_graph,
            "movies_json_lines_cleaned_names",
            "movie_graph"
        )
    ])