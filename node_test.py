"""
Tests for node module

From project root:
    pytest -s
    pytest -s node_test.py
    pytest -s -vvv node_test.py::NodeTest::test_equality
"""
import copy
from pathlib import Path
from typing import Tuple
from unittest import TestCase

from customs import Customs, FileType
from generator import CASE_INFO
from node import TreeNode, Node


class NodeTest(TestCase):
    
    def test_equality(self):
        """ Examples of valid equality tests for NamedTuples """
        p1 = Path.home()
        n1a = Node.new(p1)
        n1b = Node.new(p1)
        p2 = Path.cwd()
        n2a = Node.new(p2)
        n2b = Node.new(p2)
        
        # NamedTuples implement compare
        assert n1a == n1b
        assert n1a != n1b._replace(id=5)

        # The hash provides good "semantic" identity
        assert 1 == len({n1a, n1a, n1b})

        # If placed in sets, the sets are comparable
        assert {n1a} == {n1b}
        assert {n1a} != {n1b._replace(id=5)}
        assert {n1a, n2a} == {n1b, n2b}
        
        # dict compare works as expected
        d1 = {n1a.id: n1a, n2a.id: n2a}
        d2 = {n2a.id: n2a, n1a.id: n1a}
        
        assert d1 == d2
        d2[n2a.id] = n2a._replace(tag="foo")
        assert d1 != d2
        
        # lists are order sensitive
        l1 = [n1a, n2a]
        l2 = [n1a, n2a]
        assert l1 == l2
        l2 = [n2a, n1a]
        assert l1 != l2
        
        # we can cure that with a sort
        assert sorted(l1) == sorted(l2)
        # or a set
        assert 2 == len(set(l1))
        assert set(l1) == set(l2)


class TreeNodeTest(TestCase):

    def _treenode_counts_recurse(self, tn: TreeNode) -> Tuple[int, int]:
        dirs = len(tn.dirs)
        files = len(tn.files)
        for d in tn.dirs:
            d, f = self._treenode_counts_recurse(d)
            dirs += d
            files += f
        return dirs, files

    def _treenode_counts(self, tn: TreeNode) -> Tuple[int, int]:
        dirs, files = self._treenode_counts_recurse(tn)
        return dirs + 1, files

    def test_equality(self):
        """ Examples of valid equality tests for NamedTuples """
        p1 = Path.home()
        p2 = Path.cwd()
        n1a = TreeNode.new(p1)
        n1b = TreeNode.new(p1)
        n2a = TreeNode.new(p2)
        n2b = TreeNode.new(p2)
        for tn in (n1a, n1b, n2a, n2b):
            tn.add(Path("./data"))
            tn.add(Path("requirements.txt"))
        
        # NamedTuples implement compare
        assert n1a == n1b
        assert n1a != n1b._replace(files=[])
        
        # Deep compare works
        case = "case_100"
        tn1 = Customs(case, FileType.PICKLE).read()
        tn2 = copy.deepcopy(tn1)

        assert tn1 == tn2
        # Fragile - this is tied to case_100 dir list order
        tn2.dirs[16].dirs[0].files.append('foo')
        assert tn1 != tn2

        # BUT, when a NamedTuple contains a list, you cannot use it in a set.
        #   TypeError: unhashable type: 'list'
        # We can't make files, dirs a tuple, because NamedTuple._replace returns
        # a copy of NamedTuple - to big of a perf hit for a TreeNode
        # assert 1 == len({n1a, n1a, n1b})

        # If placed in sets, the sets are comparable
        # assert {n1a} == {n1b}
        # assert {n1a} != {n1b._replace(files=[])}
        # assert {n1a, n2a} == {n1b, n2b}

    def test_node_counts(self):
        case = "case_100"
        tn = Customs(case, FileType.PICKLE).read()
        
        # Verify counts match case info
        dirs, files = tn.node_counts()
        assert dirs == CASE_INFO[case]['dirs']
        assert files == CASE_INFO[case]['files']
        
        # Verify by manual counting
        dirs, files = self._treenode_counts(tn)
        assert dirs == CASE_INFO[case]['dirs']
        assert files == CASE_INFO[case]['files']

    def test_to_dict(self):
        case = "case_100"
        tn = Customs(case, FileType.PICKLE).read()
        tn_dict = tn.to_id_dict()

        # Verify same item count in tn and dict
        dirs, files = tn.node_counts()
        assert dirs + files == len(tn_dict)

        # Verify each node in tn is also in dict
        for item in tn.iter():
            assert item.me.id in tn_dict
            for f in item.files:
                assert f.id in tn_dict
    
    def test_treenode_iter(self):
        case = "case_100"
        tn = Customs(case, FileType.PICKLE).read()
        # Every dir is wrapped in a TreeNode - these should be equal
        assert sum(1 for _ in tn.iter()) == CASE_INFO[case]['dirs']
