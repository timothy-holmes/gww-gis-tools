import json
from collections import defaultdict

import pandas as pd
import pytest
from gww_gis_tools.trace_gis import trace_sewer


@pytest.fixture()
def sample_pipe_dicts(sample_data):
    return sample_data.get('pipes')


@pytest.fixture()
def sample_pipe_df(sample_data):
    return sample_data.get('pipes')


def test_sample_df(sample_pipe_df: pd.DataFrame):
    for c in ['PIPE_ID', 'START_NODE', 'END_NODE']:
        assert c in sample_pipe_df.columns, ''


def test_init():
    g_u = trace_sewer.Graph(trace_sewer.DIRECTION.U)
    assert type(g_u.direction) is trace_sewer.DIRECTION
    assert type(g_u.nodes) is defaultdict
    assert type(g_u.pipes) is defaultdict
    assert type(g_u.nodes['random_key']) == []
    g_d = trace_sewer.Graph(trace_sewer.DIRECTION.D)
    assert type(g_d.direction) is trace_sewer.DIRECTION
    assert type(g_d.nodes) is defaultdict
    assert type(g_d.pipes) is defaultdict
    assert type(g_d.nodes['random_key']) == []


def test_add_edge():
    g_u = trace_sewer.Graph(trace_sewer.DIRECTION.U)
    g_u = g_u.add_edge('NodeA', 'NodeB', 'PipeA')
    assert g_u.pipes


def from_gdf(sample_pipe_df: pd.DataFrame):
    g_u = trace_sewer.Graph(trace_sewer.DIRECTION.U)
    g_u = g_u.from_gdf(sample_pipe_df)

    # all the pipes have been entered (+ no PIPE_ID duplicates)
    assert len(g_u.pipes) == len(sample_pipe_df.index)

    # all the unique nodes have been entered
    assert len(g_u.nodes) == len(
        set(sample_pipe_df.START_NODE) | set(sample_pipe_df.END_NODE)
    )

    # aal the connections enetered in the hash tables
    assert len(list(g_u.nodes.values())) == len(sample_pipe_df.index)
    assert len(list(g_u.pipes.values())) == len(sample_pipe_df.index)


def to_file(sample_pipe_dicts, sample_data_path):
    # setup new graph
    g = trace_sewer.Graph(trace_sewer.DIRECTION.U)
    g = g.from_dicts(sample_pipe_dicts)

    # reference graph (serialised)
    with open(sample_data_path + 'test_graph_file.json') as f:
        sample_g_serialised = f.read()

    g_serialised = json.dumps(g, cls=trace_sewer.ExtendedEncoder, sort_keys=True)

    assert g_serialised == sample_g_serialised


def from_file(sample_pipe_dicts, sample_data_path):
    # setup new graph
    g = trace_sewer.Graph(trace_sewer.DIRECTION.U)
    g = g.from_dicts(sample_pipe_dicts)

    # reference graph (serialised)
    with open(sample_data_path + 'test_graph_file.json') as f:
        g_unserialised = json.load(f, cls=trace_sewer.ExtendedDecoder)

    assert g.direction == g_unserialised.direction
    assert g.nodes == g_unserialised.nodes
    assert g.pipes == g_unserialised.pipes
