#!/usr/bin/env python3
"""
I control the import and export of things (to file)

Serialization concerns
- speed
- size - only large differences will matter - perhaps 20%
- model version flexibility - what happens when we change the model
    - change model code - e.g. pickle stores the actual object, if we fix a bug, we have
      to translate all existing archives
    - change field order - if we don't store field name information, do we break all existing archives
    - add/remove fields - no field names makes this difficult

Lists of protocols
- protobuf and other fancy things https://gist.github.com/monkeybutter/b91004077be5d73a478a
- OLD https://gist.github.com/cactus/4073643
"""
import argparse
import csv
import json

import simplejson
import ujson
import cbor2
import cbor
import pickle
from argparse import RawDescriptionHelpFormatter
from collections import defaultdict
from enum import Enum
from pathlib import Path
from timeit import default_timer as timer
from typing import Dict, Union, Callable

import msgpack

from node import Node, TreeNode


class FileType(Enum):
    """
    I define known formats and provide some helper methods
    In [20]: FileType.PICKLE.path('case_100')
    Out[20]: './data/pickle/case_100.pickle'
    """
    PICKLE = 'pickle'
    CBOR = 'CBOR'
    CBOR2 = 'CBOR2'
    CSV = 'csv'
    JSON = 'json'
    MSGPACK = 'msgpack'
    PROTOBUF = 'protobuf'
    SIMPLEJSON = 'simplejson'
    UJSON = 'ujson'
    
    @staticmethod
    def all():
        return [x for x in FileType.__members__.values()]

    @staticmethod
    def all_but_pickle():
        return [x for x in FileType.__members__.values() if x != FileType.PICKLE]
    
    @staticmethod
    def best():
        poor_perf = [FileType.CSV, FileType.CBOR, FileType.CBOR2]
        return [x for x in FileType.__members__.values() if x not in poor_perf]
    
    def path(self, stem: str) -> str:
        return f"./data/{self.value}/{stem}.{self.value}"
    
    def exists(self, stem: str) -> bool:
        return Path(self.path(stem)).exists()


class NodeStats:
    def __init__(self):
        def new_key():
            return dict(files=0, dirs=0)
        self.stats = defaultdict(new_key)

    def _calculate_treenode(self, label: str, collection: TreeNode):
        # TODO: make TreeNode support iter()
        if not collection.me.parent_id:
            self.stats[label]['dirs'] = 1
        self.stats[label]['dirs'] += len(collection.dirs)
        self.stats[label]['files'] += len(collection.files)
        for d in collection.dirs:
            self._calculate_treenode(label, d)

    def _calculate_dict(self, label: str, collection: Dict):
        for node in collection.values():
            if node.is_dir():
                self.stats[label]['dirs'] += 1
            else:
                self.stats[label]['files'] += 1

    def add(self, label: str, collection: Union[Dict, TreeNode]):
        if isinstance(collection, TreeNode):
            self._calculate_treenode(label, collection)
        else:
            self._calculate_dict(label, collection)

    def report(self):
        print("Label\tNodes\tDirs\tFiles")
        for k,v in self.stats.items():
            print(f"{k}\t{v['dirs'] + v['files']}\t{v['dirs']}\t{v['files']}")

class Customs:
    """
    I import and export TreeNodes, id_dict
    Assumptions:
    - on write, the treenode and id_dict have already been generated
    
    NOTES:
    - We need a TreeNode structure so descendents can inherit properties
    - This class is used in benchmarking serialization - completely writing and reading
      to restore both the TreeNode and id_dict. Converting to a serialization form and
      reconstructing both collections is included in the cost for each encoding.
    """
    
    def __init__(self, stem: str, source_kind: FileType):
        self.stem = stem
        self.filetype = source_kind
        
        # Our 3 data formats are views into the same collection of Nodes for size/speed
        # - change one Node, change all collections
        self.treenode: TreeNode = None
        # Node.id -> Node
        self.id_dict: Dict[int, Node] = {}
        # Node.id -> TreeNode (only dirs)
        self.tn_dict: Dict[int, TreeNode] = {}
        
        # Storing data as a list is 2-3 times slower because of all the dict constructs
        # For simplejson, it does not matter because it stores key names when archiving NT,
        # but it is always slower than others
        self.json_data_is_list = True
    
    def _path(self, kind: FileType=None) -> str:
        if not kind:
            kind = self.filetype
        return kind.path(self.stem)
    
    def to_dict_list(self):
        # TODO: If we choose a json, we could manually code a Node export format to avoid dict construction and encoding
        return [x._asdict() for x in self.id_dict.values()]

    def _json_read(self, fn: str, load_func: Callable) -> Dict[int, Node]:
        """
        Consolidate json logic here - so we can change it on all for any particular run
        """
        with open(fn, "r") as f:
            self.id_dict = {}
            if self.json_data_is_list:
                for item in load_func(f):
                    self.id_dict[item['id']] = Node(**item)
            else:
                for v in load_func(f).values():
                    self.id_dict[v[0]] = Node._make(v)
        return self.id_dict

    def _json_dump(self, fn: str, dump_func: Callable) -> None:
        """
        Consolidate json logic here - so we can change it on all for any particular run

        python's built in json looks to be the fastest of its type, but still 1/4 speed of pickle
        Options
        - Use the id_dict: NamedTuples serialize without key names so encoded data is a list of fields.
          Every time we add/remove/change name/ change order, we break all archives - Basically the same
          problems as pickle (except that encodes the structures as well).
          BUT - it is 2x perf
        - Use a list of NT._asdict. Slows speed to 1/2 of id_dict approach, but solves robustness problems
        - Refactor out the NamedTuples. This avoids all the _asdict constructions, but living with it
          would be much less pleasant
        - Implement a c extension. What would we fix?

        NOTE: this remains constant perf for all test cases. It beats pickle on the home case
        TODO: decide on a best approach
        """
        with open(fn, "w") as f:
            if self.json_data_is_list:
                # Slower because of all the dict constructs
                dump_func(self.to_dict_list(), f)
            else:
                dump_func(self.id_dict, f)

    def read(self) -> Union[Dict, TreeNode]:
        """
        I return the best representation the source format supports
        pickle: TreeNode
        else  : Dict[inode -> properties]
        """
        fn = self._path()
        if self.filetype == FileType.PICKLE:
            with open(fn, "rb") as f:
                self.treenode = pickle.load(f)
                return self.treenode
        elif self.filetype == FileType.CSV:
            self.id_dict = {}
            with open(fn, "r") as f:
                r = csv.DictReader(f)
                for line in r:
                    # type conversion
                    for field in [k for k,v in Node._field_types.items() if v != str]:
                        line[field] = int(line[field])
                    self.id_dict[int(line['id'])] = Node(**line)
                return self.id_dict
        elif self.filetype == FileType.MSGPACK:
            # TODO: This will fail with larger files - have to adjust max_xxx_len
            with open(fn, "rb") as f:
                self.id_dict = {}
                for item in msgpack.unpack(f, raw=False):
                    self.id_dict[item['id']] = Node(**item)
            return self.id_dict
        elif self.filetype == FileType.JSON:
            return self._json_read(fn, json.load)
        elif self.filetype == FileType.UJSON:
            return self._json_read(fn, ujson.load)
        elif self.filetype == FileType.SIMPLEJSON:
            # NOTE: simplejson includes key names when serializing NamedTuples
            with open(fn, "r") as f:
                self.id_dict = {}
                if self.json_data_is_list:
                    for item in simplejson.load(f):
                        self.id_dict[item['id']] = Node(**item)
                else:
                    for v in simplejson.load(f).values():
                        self.id_dict[v['id']] = Node(**v)
            return self.id_dict
        elif self.filetype == FileType.CBOR2:
            with open(fn, "rb") as f:
                self.id_dict = {}
                for item in cbor2.load(f):
                    self.id_dict[item['id']] = Node(**item)
            return self.id_dict
        elif self.filetype == FileType.CBOR:
            with open(fn, "rb") as f:
                self.id_dict = {}
                for item in cbor.load(f):
                    self.id_dict[item['id']] = Node(**item)
            return self.id_dict

    def write(self, kind: FileType) -> None:
        fn = self._path(kind)
        if kind == FileType.PICKLE:
            # serialize as TreeNode
            with open(fn, "wb") as f:
                pickle.dump(self.treenode, f, protocol=-1)
        elif kind == FileType.CSV:
            # serialize as id_dict
            with open(fn, "w") as f:
                w = csv.DictWriter(f, Node._fields)
                w.writeheader()
                for item in self.treenode.node_iter():
                    w.writerow(item._asdict())
        elif kind == FileType.MSGPACK:
            # https://msgpack-python.readthedocs.io/en/latest/api.html
            with open(fn, "wb") as f:
                # Doesn't improve speed
                # msgpack.pack(self._to_dict(), f, use_bin_type=True)
                msgpack.pack(self.to_dict_list(), f)
        elif kind == FileType.JSON:
            self._json_dump(fn, json.dump)
        elif kind == FileType.UJSON:
            self._json_dump(fn, ujson.dump)
        elif kind == FileType.SIMPLEJSON:
            # NOTE: simplejson includes key names when serializing NamedTuples
            with open(fn, "w") as f:
                if self.json_data_is_list:
                    simplejson.dump(list(self.id_dict.values()), f)
                else:
                    simplejson.dump(self.id_dict, f)
        elif kind == FileType.CBOR2:
            with open(fn, "wb") as f:
                cbor2.dump(self.to_dict_list(), f)
        elif kind == FileType.CBOR:
            with open(fn, "wb") as f:
                cbor.dump(self.to_dict_list(), f)
        # TODO: protobuf: https://developers.google.com/protocol-buffers/docs/pythontutorial
        #       pyrobuf
        # TODO: Thrift?
        # TODO: arrow?
        # TODO: python-rapidjson
        # TODO: bson
        # TODO: HDF5

    def translate(self):
        """
        We serialize either a TreeNode or an id_dict. After reading, call
        this method to ensure all source formats are available.
        Assumes we have
        """
        if self.treenode:
            # Create id_dict from TreeNode - only pickle
            self.id_dict = self.treenode.to_id_dict()
            self.tn_dict = self.treenode.to_tn_dict()
        elif self.id_dict:
            # Create TreeNode from id_dict - all other serializations
            # If serialization produces an id_dict, we must create the tn_dict
            self.tn_dict = {}
            for id, node in self.id_dict.items():
                if node.is_dir():
                    self.tn_dict[id] = TreeNode(me=node, files=[], dirs=[])
                    
            # Define dir hierarchy
            # Also identify root node to remove one tn_dict traversal
            for tn in self.tn_dict.values():
                if not tn.me.parent_id:
                    self.treenode = tn
                else:
                    self.tn_dict[tn.me.parent_id].dirs.append(tn)
                
            # Merge files into dir hierarchy
            for node in self.id_dict.values():
                if not node.is_dir():
                    self.tn_dict[node.parent_id].files.append(node)
        else:
            raise ValueError("No internal format to translate.")


def help():
    return """Customs controls file import and export

You can also use this script to translate one format to another and validate the translation

USE:
    Convert the case_100 test case from pickle to csv
      ./customs.py --case case_100 --import pickle --export csv
      ./customs.py --case case_5000 --import pickle --export csv
"""


def main():
    parser = argparse.ArgumentParser(description=help(), formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--case',
                       required=True,
                       help='Use case - file stem used in import/export. In file "case_100.csv", "case_100" is the stem.')
    
    group = parser.add_argument_group()
    group.add_argument('-i', '--import-type',
                       required=True,
                       help='import file type')
    group.add_argument('-e', '--export-type',
                       required=True,
                       help='export file type')
    
    parser.add_argument('-v', '--validate',
                       action='store_true',
                       default=False,
                       help='print stats for both files')

    args = parser.parse_args()

    if args.import_type.upper() not in FileType.__members__:
        print(f"import-type must be one of {', '.join(FileType.__members__)}")
        exit(1)

    if args.export_type.upper() not in FileType.__members__:
        print(f"export-type must be one of {', '.join(FileType.__members__)}")
        exit(1)

    ift = FileType(args.import_type)
    if not ift.exists(args.case):
        print(f"Import file must exist: {ift.path(args.case)}")
        exit(1)

    c = Customs(args.case, ift)
    start = timer()
    c1 = c.read()
    end = timer()
    print(f"Read {ift.path(args.case)} in {end-start:.3f} seconds")

    eft = FileType(args.export_type)
    start = timer()
    c.write(eft)
    end = timer()
    print(f"Wrote {eft.path(args.case)} in {end - start:.3f} seconds")

    if args.validate:
        c = Customs(args.case, eft)
        c2 = c.read()
        ns = NodeStats()
        ns.add(f"{args.case}.{args.import_type}", c1)
        ns.add(f"{args.case}.{args.export_type}", c2)
        ns.report()
        

if __name__ == "__main__":
    main()

