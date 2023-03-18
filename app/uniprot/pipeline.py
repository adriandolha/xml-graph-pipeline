from __future__ import annotations
import os.path
import shutil
import uuid
from enum import Enum
from typing import List, Dict
from xml.etree import ElementTree
from dataclasses import dataclass, field, asdict
from py2neo import Graph, Node, Relationship


class NodeType(str, Enum):
    ENTRY = 'entry'
    PROTEIN = 'protein'
    GENE = 'gene'
    ORGANISM = 'organism'


class DataMixin:
    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)


@dataclass
class Protein(DataMixin):
    full_name: str
    etype: NodeType

    @staticmethod
    def from_xml(elem: Elem):
        full_name = 'n/a'
        recommendedName = elem.find('recommendedName')
        if recommendedName is not None:
            fullName = recommendedName.find('fullName')
            if fullName is not None:
                full_name = fullName.text
        return Protein(full_name=full_name, etype=NodeType.PROTEIN)


@dataclass
class Gene(DataMixin):
    name: str
    etype: NodeType

    @staticmethod
    def from_xml(elem: Elem):
        name = ''
        for c in elem.raw_children():
            if c.attrib.get('type') == 'primary':
                name = c.text

        return Gene(name=name, etype=NodeType.GENE)


@dataclass
class Entry:
    proteins: List[Protein] = field(default_factory=list)
    genes: List[Gene] = field(default_factory=list)
    organisms: List[Organism] = field(default_factory=list)
    etype: NodeType = NodeType.ENTRY

    @staticmethod
    def from_xml(elem: Elem):
        children = elem.children()
        proteins = [c for c in children if c is not None and c.etype == NodeType.PROTEIN]
        genes = [c for c in children if c is not None and c.etype == NodeType.GENE]
        organisms = [c for c in children if c is not None and c.etype == NodeType.ORGANISM]
        return Entry(proteins=proteins, genes=genes, organisms=organisms)

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> Entry:
        proteins = [Protein.from_dict(e) for e in data['proteins']]
        genes = [Gene.from_dict(e) for e in data['genes']]
        organisms = [Organism.from_dict(e) for e in data['organisms']]
        return Entry(proteins=proteins, genes=genes, organisms=organisms)

    def to_neo(self):
        items = []
        protein = self.proteins[0]
        protein_node = Node(NodeType.PROTEIN.value)
        items.append(protein_node)
        full_name_node = Node('FullName', name=protein.full_name)
        items.extend([full_name_node, Relationship(protein_node, 'HAS_FULL_NAME', full_name_node)])
        for gene in self.genes:
            gene_node = Node(NodeType.GENE.value, name=gene.name)
            items.extend([gene_node, Relationship(protein_node, 'HAS_GENE', gene_node)])
        for organism in self.organisms:
            organism_node = Node(NodeType.ORGANISM.value, name=organism.name, taxonomy_id=organism.taxonomy_id)
            items.extend([organism_node, Relationship(protein_node, 'IN_ORGANISM', organism_node)])

        return items


@dataclass
class Organism(DataMixin):
    name: str
    taxonomy_id: int
    etype: NodeType

    @staticmethod
    def from_xml(elem: Elem):
        name, taxonomy_id = '', -1
        for c in elem.raw_children():
            if c.tag == 'name' and c.attrib.get('type') == 'scientific':
                name = c.text
            if c.tag == 'dbReference' and c.attrib.get('type') == 'NCBI Taxonomy':
                taxonomy_id = int(c.attrib['id'])

        return Organism(name=name, taxonomy_id=taxonomy_id, etype=NodeType.ORGANISM)


class Elem:
    def __init__(self, elem: ElementTree):
        self.e = elem
        self.url = '{http://uniprot.org/uniprot}'

    @property
    def tag(self):
        return self.e.tag.replace('{http://uniprot.org/uniprot}', '')

    @property
    def text(self):
        return self.e.text

    def find(self, tag):
        return Elem(self.e.find(f'{self.url}{tag}'))

    @property
    def attrib(self):
        return self.e.attrib

    @property
    def protein(self):
        return

    def parse(self):
        parsers = {
            'protein': Protein,
            'entry': Entry,
            'gene': Gene,
            'organism': Organism,
        }
        parser = parsers.get(self.tag)
        return parser.from_xml(self) if parser is not None else None

    def raw_children(self) -> List[Elem]:
        return [Elem(c) for c in self.e]

    def children(self) -> List:
        return [Elem(c).parse() for c in self.e]


def parse_xml(xml_file_path):
    print(f'Parsing XML {xml_file_path}...')
    tree = ElementTree.parse(xml_file_path)
    root = tree.getroot()
    data = [Elem(e).parse() for e in root]
    return list(filter(lambda p: p is not None and type(p) is Entry, data))


def load(entries: List[Entry]):
    print('Loading data into Neo4j...')
    print(entries)
    graph = Graph()
    for entry in entries:
        if entry is not None and type(entry) is Entry:
            for item in entry.to_neo():
                graph.create(item)


def delete_data():
    print('Delete Neo4j data...')
    Graph().delete_all()


def generate_data(no_of_files):
    files = []
    target_dir = os.path.expanduser(f'~/uniprot_data')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    for _ in range(no_of_files):
        src_file_name = os.path.expanduser(f'~/data_samples/uniprot.xml')
        dst_file_name = os.path.expanduser(f'{target_dir}/uniprot_{str(uuid.uuid4())}.xml')
        print(src_file_name, dst_file_name)
        shutil.copy(src_file_name, dst_file_name)
        files.append(dst_file_name)
    return files


def move_parsed_xml(xml_file_path):
    target_dir = os.path.expanduser(f'~/uniprot_data_processed')
    print(f'Moving {xml_file_path} to {target_dir}...')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    target_path = os.path.join(target_dir, os.path.basename(xml_file_path))
    if os.path.exists(target_path):
        os.remove(target_path)
    shutil.move(xml_file_path, target_dir)


def data_to_dict(data: List[Entry]):
    return [e.to_dict() for e in data]


def data_from_dict(data: dict) -> List[Entry]:
    return [Entry.from_dict(e) for e in data]
