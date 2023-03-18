import os
import pytest
from py2neo import Graph

from uniprot import pipeline
from uniprot.pipeline import NodeType, Entry


@pytest.fixture()
def data_path():
    yield os.path.expanduser('~/data_samples/data.xml')


@pytest.fixture()
def clean_graph():
    pipeline.delete_data()


class TestApp:
    def test_uniprot(self, data_path, clean_graph):
        data = pipeline.parse_xml(data_path)
        for e in data:
            assert e is not None and type(e) is Entry
            print(e)
            if e.etype == NodeType.ENTRY:
                for protein in e.proteins:
                    assert protein.full_name is not None
                    assert protein.full_name != "n/a"
        data_dict = pipeline.data_to_dict(data)
        print(data_dict)
        pipeline.load(pipeline.data_from_dict(data_dict))

    def test_generate_data(self, data_path):
        pipeline.generate_data(1)

    def test_move_parsed_data(self, data_path):
        files = pipeline.generate_data(2)
        print(files)
        for file in files:
            assert os.path.exists(file)
        for file in files:
            pipeline.move_parsed_xml(file)
        for file in files:
            assert not os.path.exists(file)
