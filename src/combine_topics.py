import numpy as np
import pandas as pd

class Graph:
    def __init__(self, size: int):
        self.vertices = [[] for i in np.arange(size)]
        self.edges = []
        self.committed = True
    
    def add_edge(self, u: int, v: int):
        self.vertices[u].append(v)
        self.vertices[v].append(u)
        self.committed = False

    def commit_edges(self):
        if self.committed:
            return
        for i in range(len(self.vertices)):
            for dest in self.vertices[i]:
                self.edges.append([i, dest])
        self.committed = True
    
    def print(self):
        for adj_list in self.vertices:
            print(adj_list)
    
    def equals(self, other):
        if len(self.vertices) != len(other.vertices):
            return False
        for i in range(len(self.vertices)):
            if set(self.vertices[i]) != set(other.vertices[i]):
                return False
        return True


def create_topic_graph(url_lists: list[list[str]], min_overlap: float) -> list[list[int]]:
    """Using graph class above to create a graph of topics based on the overlap between them."""
    overlap_graph = Graph(len(url_lists))
    for i in np.arange(len(url_lists)):
        topic_set_i = set(url_lists[i])
        for j in np.arange(i + 1, len(url_lists)):
            topic_set_j = set(url_lists[j])
            overlap_coeff= len(topic_set_i.intersection(topic_set_j))/min(len(topic_set_i), len(topic_set_j))
            if overlap_coeff >= min_overlap:
                overlap_graph.add_edge(i, j)
    overlap_graph.commit_edges()
    return overlap_graph

def dfs_connections(graph: Graph, start: int, visited: set):
    """Depth first search on the graph."""
    # not sure why defaulting to set() here causes visited to carry over from previous calls
    visited.add(start)
    for dest in graph.vertices[start]:
        if dest not in visited:
            visited = dfs_connections(graph, dest, visited)
    return visited

def combine_topics(topic_list: list[str], overlap_graph: Graph, topic_set: set) -> dict[str, str]:
    """Combine topics based on the topic_matrix and topic_set."""
    # DFS to find connected components
    groups = []
    grouped_nodes = set()
    for i in range(len(topic_list)):
        if i not in grouped_nodes:
            new_group = dfs_connections(overlap_graph, i, set())
            grouped_nodes = grouped_nodes.union(new_group)
            topic_group = [topic_list[j] for j in new_group]
            groups.append(set(topic_group))
    # Deciding which topic should represent each group
    topic_remap = {}
    for group in groups:
        common_topics = group.intersection(topic_set)
        group_list = list(group)
        umbrella = None
        if len(common_topics) == 1:
            umbrella = common_topics.pop()
        else:
            # does this cover both == 0 and > 1?
            common_list = list(common_topics) if len(common_topics) > 1 else group_list
            scores = np.zeros(len(common_list))
            for i in range(len(common_list)):
                word_set = set(common_list[i].split())
                score = 0
                for topic in group_list:
                    if common_list[i] != topic:
                        group_word_set = set(topic.split())
                        score += len(word_set.intersection(group_word_set))
                scores[i] = score
            max_index = np.argmax(scores)
            umbrella = common_list[max_index]
        for topic in group_list:
            topic_remap[topic] = umbrella
    return topic_remap

def score_func(row: pd.Series) -> float:
    """Calculate geometric mean logged for topic."""
    return np.log(row['counts']) + 2 * np.log(row['src_counts']) + 1

def topic_agg_func(df) -> pd.DataFrame:
    """Aggregate the dataframe."""
    new_url_list = []
    new_src_list = []
    tuple_list = []
    for url_list, src_list in zip(df['urls'], df['sources']):
        for url, src in zip(url_list, src_list):
            tuple_list.append((src, url))
    tuple_list = list(set(tuple_list))
    for pair in tuple_list:
        new_src_list.append(pair[0])
        new_url_list.append(pair[1])
    new_df = pd.DataFrame({
        'topics': df['topic'],
        'sources': new_src_list,
        'urls': new_url_list,
        'counts': len(new_url_list),
        'src_counts': len(set(new_src_list)),
    })
    return new_df
def combine_df_topics(df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    url_list = df.urls.tolist()
    topic_list = df.topics.tolist()
    topic_graph = create_topic_graph(url_list, 0.75)
    old_set = set(old_df['topics'].tolist()) if old_df is not None else set()
    topic_remap = combine_topics(topic_list, topic_graph, old_set)
    df['topics'] = df['topics'].apply(lambda x: topic_remap[x])
    df.groupby('topics').agg(topic_agg_func)
    df['scores'] = df.apply(score_func, axis=1)
    return df

