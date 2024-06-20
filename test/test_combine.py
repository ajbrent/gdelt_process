from src import utils
from src.combine_topics import Graph
from src import combine_topics
import pytest

def graph_from_adj_list(adj_list: list[list[int]]) -> Graph:
    graph = Graph(len(adj_list))
    for i in range(len(adj_list)):
        for j in adj_list[i]:
            graph.add_edge(i, j)
    graph.commit_edges()
    return graph

def test_create_topic_graph():
    url_lists = [
        ['a', 'b', 'c', 'e', 'f'],
        ['a', 'b'],
        ['a', 'b', 'c', 'd', 'g'],
        ['h', 'i', 'j', 'k'],
        ['h', 'i', 'l', 'm'],
        ['h', 'i'],
        ['n', 'o', 'p', 'q'],
        ['p', 'q']
    ]
    adj_list = [
        [1],
        [0, 2],
        [1],
        [5],
        [5],
        [3, 4],
        [7],
        [6],
    ]
    expected_graph = graph_from_adj_list(adj_list)
    actual_graph = combine_topics.create_topic_graph(url_lists, 0.75)
    assert expected_graph.equals(actual_graph)
    assert actual_graph.equals(expected_graph)

def test_dfs_connections():
    adj_list = [
        [1],
        [0, 2],
        [1],
        [5],
        [5],
        [3, 4],
        [7],
        [6],
    ]
    graph = graph_from_adj_list(adj_list)
    assert combine_topics.dfs_connections(graph, 0, set()) == {0, 1, 2}
    assert combine_topics.dfs_connections(graph, 3, set()) == {3, 5, 4}
    assert combine_topics.dfs_connections(graph, 6, set()) == {6, 7}

def test_combine_topics():
    topic_list = ['z', 'y', 'x y z', 'w', 'v w u', 'u', 't', 's']
    topic_set = {'z', 's'}
    adj_list = [
        [1],
        [0, 2],
        [1],
        [5],
        [5],
        [3, 4],
        [7],
        [6],
    ]
    graph = graph_from_adj_list(adj_list)
    topic_map = combine_topics.combine_topics(topic_list, graph, topic_set)
    expected_topic_map = {
        'z': 'z',
        'y': 'z',
        'x y z': 'z',
        'w': 'v w u',
        'v w u': 'v w u',
        'u': 'v w u',
        't': 's',
        's': 's'
    }
    assert topic_map == expected_topic_map