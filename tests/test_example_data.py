def test_sample_data(sample_data):
    d = sample_data
    assert d


def test_pipes(sample_data):
    assert 'pipes' in sample_data
    pipes = sample_data['pipes']
    assert not pipes.empty
    assert 'start_node' in pipes.columns
    assert 'end_node' in pipes.columns
    assert 'pipe_id' in pipes.columns


def test_nodes(sample_data):
    assert 'nodes' in sample_data
    nodes = sample_data['nodes']
    assert not nodes.empty
    assert 'node_id' in nodes.columns


def test_branches(sample_data):
    assert 'branches' in sample_data
    branches = sample_data['branches']
    assert not branches.empty
    assert 'pipe_id' in branches.columns
