#!/usr/bin/env python3
"""
The bench runs and reports on ingest scripts

Questions
- What is the fastest way to ingest n nodes, relationships?
- Does that vary by collection size?

Learnings:
- a statement is terminated by a ';' - there can be only one per run()
- many clauses can be in a single statement (CREATE, MERGE, etc)
- we jam as many clauses into a statement cause each run() has an auto commit - slow if we don't
- a merge either matches everything, or tries to create everything in the pattern.
- WITH rescopes variables groups some collections - probably no use in ingestion because we
  have many clauses it would affect

TODO:
    - validate results
    - sprayers - session pool that suports unordered data, but also indexing cause it is MERGE
"""
import argparse
from argparse import RawDescriptionHelpFormatter
from enum import Enum
from pprint import pformat, pprint
from timeit import default_timer as timer
from typing import List

from customs import Customs, FileType
from generator import CASE_INFO


class BenchType(Enum):
    READ = 'read'
    WRITE = 'write'
    
    
class Bench:
    """
    I time things and report
    """
    def __init__(self, kind: BenchType, case: str, file_types: List[FileType], iterations: int):
        self.kind: BenchType = kind
        self.case: str = case
        self.files_types: List[FileType] = file_types
        self.iterations: int = iterations
        self.stats: List[str] = ["Case\tNodes\tDuration\tNodes/sec"]

    def _add_stat(self, kind: FileType, duration: float) -> None:
        nc = CASE_INFO[self.case]['nodes']
        nps = int(nc / duration)
        self.stats.append(f"{self.case}_{kind:18}\t{nc}\t{duration:.4f}\t{nps:>6}")

    def _create_read_targets(self, overwrite=False):
        if overwrite or not all(ft.exists(self.case) for ft in self.files_types):
            c = Customs(self.case, FileType.PICKLE)
            c.read()
            c.translate()
            for ft in [x for x in self.files_types if x != FileType.PICKLE]:
                if overwrite or not ft.exists(self.case):
                    print(f"Writing {ft.path(self.case)}")
                    c.write(ft)
                
    def report(self):
        print("\n".join(self.stats))
        
    def timeit(self) -> None:
        if self.kind == BenchType.READ:
            self._time_read()
    
    def _time_read(self):
        self._create_read_targets(True)
        for ft in self.files_types:
            c = Customs(self.case, ft)
            print(f"Intermediate times for {ft}:")
            duration = 0
            for _ in range(self.iterations):
                start = timer()
                c.read()
                # TODO: add translate to better simulate actual operations
                end = timer()
                print(f"  {end-start:.3f}")
                duration += end - start

            self._add_stat(ft, duration / self.iterations)

    def validate(self):
        """
        Validate every collection has identical content
        Assume pickle is the gold standard
        - each encoding's write, read ops produce pickle contents
        - each encoding's write, read ops produce the original (handled by above)
        - each collection has identical contents (TreeNode, id_dict)
        """
        self.files_types = list(FileType.all())
        # Ensure we have up to date source files - pickles are created with the generator
        self._create_read_targets(True)

        c_pickle = Customs(self.case, FileType.PICKLE)
        c_pickle.read()
        c_pickle.translate()

        others = []
        for ft in FileType.all_but_pickle():
            c = Customs(self.case, ft)
            c.read()
            c.translate()
            others.append(c)

        for c in others:
            assert len(c_pickle.id_dict) == len(c.id_dict)
            assert c_pickle.id_dict == c.id_dict
            assert c_pickle.treenode == c.treenode
            assert c_pickle.tn_dict == c.tn_dict
        print("===> All formats passed validation")


def cases():
    hdr = "    Case        Nodes   Dirs   Files\n"
    return hdr + "\n".join([f"    {k:10} {v['dirs'] + v['files']:>6}  {v['dirs']:>5}  {v['files']:>6}" for k,v in CASE_INFO.items()])


def help() -> str:
    return f'''Benchmark read/write operations

USE:
    Large benchmark for all, case_home, 3 iterations
      ./bench.py --read --case case_home -t all -i3

    Benchmark reading case_home, 3 iterations for file types pickle, csv
      ./bench.py --read --case case_home -t pickle csv msgpack

    A simpler test case
      ./bench.py --read --case case_100 -t all

FILE TYPES:
    {", ".join(sorted(FileType.__members__.keys()))}

CASES:
{cases()}

NOTES:
    Exporting implies an import type of pickle
    Output is tab delimited for easy import into a spreadsheet
'''


def main():
    parser = argparse.ArgumentParser(description=help(), formatter_class=RawDescriptionHelpFormatter)
    # Type of benchmark, operation
    subjects = parser.add_mutually_exclusive_group(required=True)
    subjects.add_argument('-r', '--read',
                          action='store_true',
                          default=False,
                          help='Read data from file')
    subjects.add_argument('-w', '--write',
                          action='store_true',
                          default=False,
                          help='Write data from file')
    subjects.add_argument('-v', '--validate',
                          action='store_true',
                          default=False,
                          help='Check serialization for correctness')
    # Common params
    parser.add_argument('-i', '--iterations',
                        type=int,
                        default=1,
                        metavar="N",
                        help='How many times to run each case - results are averaged')
    parser.add_argument('-c', '--case',
                        metavar="N",
                        help='Which use cases, e.g. case_100 case_1750')
    parser.add_argument('-t', '--file_types',
                        nargs='+',
                        metavar="FT",
                        help='Which file types. E.g. pickle csv. Use "all" to cover all formats, "best" for best performers')
    args = parser.parse_args()
    
    if args.validate:
        b = Bench(BenchType.READ, "case_100", [], 1)
        b.validate()
        exit(0)
    
    file_types = []
    if 'all' in args.file_types:
        file_types = FileType.all()
    elif 'best' in args.file_types:
        file_types = FileType.best()
    else:
        for kind in args.file_types:
            if kind.upper() not in FileType.__members__:
                print(f"import-type must be one of {', '.join(FileType.__members__)}")
                exit(1)
            file_types.append(FileType(kind))
    
    if args.iterations < 1:
        print(f"Invalid iterations: {args.iterations}")
        exit(1)

    bt = BenchType.WRITE
    if args.read:
        bt = BenchType.READ

    b = Bench(bt, args.case, file_types, args.iterations)
    b.timeit()
    b.report()


if __name__ == "__main__":
    main()
