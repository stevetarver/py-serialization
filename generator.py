#!/usr/bin/env python3
"""
I create datasets of TreeNode hierarchies and pickle them for later use
"""
import argparse
import configparser
from argparse import RawDescriptionHelpFormatter
from pathlib import Path
from timeit import default_timer as timer
from typing import Set

from customs import Customs, FileType
from node import Node, TreeNode

CASE_INFO = {
    'case_proj': {'nodes': 22, 'dirs': 6, 'files': 16},
    'case_100': {'nodes': 98, 'dirs': 35, 'files': 63},
    'case_200': {'nodes': 206, 'dirs': 35, 'files': 171},
    'case_300': {'nodes': 315, 'dirs': 56, 'files': 259},
    'case_400': {'nodes': 413, 'dirs': 61, 'files': 352},
    'case_500': {'nodes': 515, 'dirs': 45, 'files': 470},
    'case_750': {'nodes': 832, 'dirs': 87, 'files': 745},
    'case_1000': {'nodes': 981, 'dirs': 103, 'files': 878},
    'case_1250': {'nodes': 1278, 'dirs': 83, 'files': 1195},
    'case_1500': {'nodes': 1540, 'dirs': 122, 'files': 1418},
    'case_1750': {'nodes': 1773, 'dirs': 141, 'files': 1632},
    'case_2000': {'nodes': 1980, 'dirs': 149, 'files': 1831},
    'case_2500': {'nodes': 2553, 'dirs': 140, 'files': 2413},
    'case_3000': {'nodes': 2971, 'dirs': 166, 'files': 2805},
    'case_4000': {'nodes': 3995, 'dirs': 216, 'files': 3779},
    'case_5000': {'nodes': 5056, 'dirs': 289, 'files': 4767},
    'case_10000': {'nodes': 9847, 'dirs': 1152, 'files': 8695},
    'case_home': {'nodes': 1030828, 'dirs': 124678, 'files': 906150},
}

# Conditionally exclude directories from target to hit goal node counts
CPYTHON_DIR_EXCLUSIONS = dict(
    case_100={'Doc', 'Include', 'Lib', 'Mac', 'Misc', 'Modules', 'Objects', 'PC', 'PCbuild', 'Parser', 'Python',
              'Tools'},
    case_200={'Doc', 'Include', 'Lib', 'Mac', 'Misc', 'Modules', 'Objects', 'PC', 'Parser', 'Python', 'Tools'},
    case_300={'Doc', 'Include', 'Lib', 'Modules', 'NEWS.d', 'Objects', 'PC', 'Parser', 'Python', 'Tools'},
    case_400={'Doc', 'Include', 'Lib', 'Modules', 'NEWS.d', 'Objects', 'Parser', 'Python', 'Tools'},
    case_500={'.git', 'Doc', 'Lib', 'Modules', 'NEWS.d', 'PC', 'PCBuild', 'Python', 'Tools'},
    case_750={'.git', 'Doc', 'Lib', 'Modules', 'NEWS.d', 'PC', 'PCbuild'},
    case_1000={'Doc', 'Lib', 'Modules', 'NEWS.d', 'PC'},
    case_1250={'Lib', 'Modules', 'PC', 'Tools', 'next'},
    case_1500={'Lib', 'Misc', 'Modules', 'PC'},
    case_1750={'Lib', 'Misc', 'PC', '_ctypes', 'clinic', 'libmpdec'},
    case_2000={'Lib', 'NEWS.d', 'PC'},
    case_2500={'Lib', 'Modules'},
    case_3000={'Lib'},
    case_4000={'Include', 'Modules', 'Objects', 'PC', 'Python', 'Tools'},
    case_5000={'PC'},
)


def collect_data_recurse(p: Path, tree_node: TreeNode, exclusions: Set[str]) -> None:
    """ Recurse dirs starting at tree_node, collecting information """
    for item in p.iterdir():
        # NOTE: If we cannot create the node, it will not be added and we will not recurse
        child = tree_node.add(item)
        if item.is_dir() and item.name not in exclusions and child:
            collect_data_recurse(item, child, exclusions)


def collect_data(p: Path, exclusions: Set[str]) -> TreeNode:
    """ Generate hierarchical file data """
    result = TreeNode.new(p)
    collect_data_recurse(p, result, exclusions)
    return result


def print_stats(root: TreeNode, case: str) -> None:
    """ Print case stats for CASE_INFO - for validating graph creation """
    files = 0
    dirs = 0
    for item in root.node_iter():
        if item.is_dir():
            dirs += 1
        else:
            files += 1
    print(f"'{case}': {{'nodes': {dirs + files}, 'dirs': {dirs}, 'files': {files}}},")


def remove_root_parent(root: TreeNode) -> TreeNode:
    # Ensure our root node does not have a parent id - to identify it as root
    od = root.me._asdict()
    od['parent_id'] = 0
    return TreeNode(Node(**od), root.files, root.dirs)


def pickle_dataset(p: Path, case: str, exclusions: Set[str]) -> None:
    root = collect_data(p, exclusions)
    print_stats(root, case)  # so you can add to CASE_INFO
    c = Customs(case, FileType.PICKLE)
    c.treenode = remove_root_parent(root)
    c.write(FileType.PICKLE)


def pickle_default_datasets() -> None:
    """ Generate default datasets, for use with other formats """
    config = configparser.ConfigParser()
    config.read("py-serialization.config")

    p = Path.cwd()
    pickle_dataset(p, "case_proj", {"data", ".git", ".idea", ".pytest_cache", "__pycache__"})

    p = Path(config['generator']['cpython'])
    for case, exclusions in CPYTHON_DIR_EXCLUSIONS.items():
        pickle_dataset(p, case, exclusions)

    p = Path(config['generator']['go'])
    pickle_dataset(p, "case_10000", set())

    p = Path.home()
    pickle_dataset(p, "case_home", {"appomni", "Library", "private", ".config", "Pictures", "Movies", "code"})
    

def dir_counts_recurse(node: TreeNode, indent: int = 0) -> None:
    """ Print all directories and node counts """
    fc = len(node.files)
    dc = len(node.dirs)
    descendants = 0
    for _ in node.node_iter():
        descendants += 1
    print(f"{dc: >4}  {fc: >4}  {descendants: >4}  {' ' * indent}/{node.me.name}")
    for d in node.dirs:
        dir_counts_recurse(d, indent + 2)


def dir_counts(p: Path) -> None:
    """
    Print all directories and node counts
    - create initial state for dir_counts_recurse recursion
    
    This aids in generating datasets with a target size
    """
    root = collect_data(p, set())
    print("  Dirs       : directories in current directory")
    print("  Files      : files in current directory")
    print("  Descendants: count of all descendants from current directory")
    print(f"Dirs Files Descendants Path")
    dir_counts_recurse(root)


def help():
    return """Collect dir/file metadata and pickle

The 'default' generation creates target sized sets of dir/file data from the cpython/go repo.
Set the paths to both in py-serialization.config

    ./generator.py -d

This generates the body of CASE_INFO. You should copy that into this file so other module's
validation and timing is correct.

You can generate additional datasets from any directory:
    ./generate.py -n my_shared_dir -r /Users/shared

You can list subtree descendents and their node counts to help develop other target sized datasets
    ./generate.py -l /Users/shared
"""


def main():
    parser = argparse.ArgumentParser(description=help(), formatter_class=RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--default',
                        action='store_true',
                        default=False,
                        help='generate default datasets (using default dir)')
    group.add_argument('-l', '--list',
                        metavar="DIR",
                        help='list node count for each dir in target dir')
    group.add_argument('-r', '--root',
                       metavar="DIR",
                       help='root directory - where to start parsing')
    
    parser.add_argument('-n', '--name',
                        default="funky-karmikel",
                        help='use case name - becomes the pickle file name')
    args = parser.parse_args()
    print(args)
    
    if args.default:
        print("===> Generating default datasets")
        pickle_default_datasets()
        print("===> REMEMBER: copy the above to generator.py CASE_INFO in case anything has changed.")
        exit(0)
    
    if args.root:
        p = Path(args.root)
        if not p.exists():
            print(f"Directory {p} does not exist. Cannot continue.")
            exit(1)
        if not p.is_dir():
            print(f"{p} is not a directory, cannot continue.")
            exit(1)
        print(f"===> Collecting {p} into a {args.name} pickle")
        start = timer()
        pickle_dataset(p, args.name, set())
        print(f"Operations completed in {timer() - start} seconds")

    if args.list:
        p = Path(args.list)
        if not p.exists():
            print(f"Directory {p} does not exist. Cannot continue.")
            exit(1)
        if not p.is_dir():
            print(f"{p} is not a directory, cannot continue.")
            exit(1)
        dir_counts(p)


if __name__ == "__main__":
    main()

