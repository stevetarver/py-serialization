#!/usr/bin/env python3
"""
I control the import and export of things (to file)
"""
import argparse
import csv
import pickle
import sys
from argparse import RawDescriptionHelpFormatter
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Optional, Any, Dict, Union, Iterable
from timeit import default_timer as timer

from node import TreeNode, Node


class FileType(Enum):
    """
    I define known formats and provide some helper methods
    In [20]: FileType.PICKLE.path('case_100')
    Out[20]: './data/pickle/case_100.pickle'
    """
    PICKLE = 'pickle'
    CSV = 'csv'
    
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
    """ I import and export TreeNodes
    TODO: We need tree node to infer properties when inherited from parent
    """
    
    def __init__(self, stem: str, source_kind: FileType):
        self.stem = stem
        self.filetype = source_kind
        self.treenode = None
        self.dict = None
    
    def _path(self, kind: FileType=None) -> str:
        if kind:
            return kind.path(self.stem)
        return self.filetype.path(self.stem)
        
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
            self.dict = {}
            with open(fn, "r") as f:
                r = csv.DictReader(f)
                for line in r:
                    self.dict[line['id']] = Node(**line)
                return self.dict
            
    def write(self, kind: FileType) -> None:
        fn = self._path(kind)
        if kind == FileType.PICKLE:
            with open(fn, "wb") as f:
                pickle.dump(self.treenode, f)
        elif kind == FileType.CSV:
            with open(fn, "w") as f:
                w = csv.DictWriter(f, Node._fields)
                w.writeheader()
                for item in self.treenode.iter():
                    w.writerow(item._asdict())

    def translate(self):
        """ Convert from TreeNode to Dict or vice versa """
        if self.treenode is None and self.dict is None:
            raise ValueError("No internal format to operate on.")
        
        if self.treenode:
            self.dict = {}
            for item in self.treenode.iter():
                self.dict[item.id] = item
        else:
            def new_key():
                return dict(files=[], dirs=[])
            temp = defaultdict(new_key)
            for node in self.dict.values():
                # Group all nodes by path
                if node.is_dir():
                    temp[node.path]['dirs'].append(node)
                else:
                    temp[node.path]['files'].append(node)
            # group all items by path and is dir
            # traverse result and build TreeNode


def help():
    return """Customs controls file import and export

You can also use this script to translate one format to another and validate the translation

USE:
    Convert the case_100 test case from pickle to csv
      ./customs.py --case case_100 --import pickle --export csv
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
        print(f"import-type must be one of {', '.join(FileType.__members__.keys())}")
        exit(1)

    if args.export_type.upper() not in FileType.__members__:
        print(f"export-type must be one of {', '.join(FileType.__members__.keys())}")
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

