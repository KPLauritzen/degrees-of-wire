import pytest

@pytest.fixture
def movie_graph(kedro_catalog):
    return kedro_catalog.load("movie_graph")

@pytest.fixture
def kedro_catalog():
    from kedro.config import ConfigLoader
    from kedro.io import DataCatalog
    conf_paths = ['conf/base', 'conf/local']
    conf_loader = ConfigLoader(conf_paths)
    conf_catalog = conf_loader.get('catalog*', 'catalog*/**')
    return DataCatalog.from_config(conf_catalog)


def test_graph_exists(movie_graph):
    assert len(movie_graph) > 0

def test_bacon_smith(movie_graph):
    import networkx as nx
    path = nx.shortest_path(movie_graph, "Kevin Smith", "Kevin Bacon")
    assert len(path) == 3