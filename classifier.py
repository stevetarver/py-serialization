#!/usr/bin/env python3
"""
A classification is implemented by a set of rules


A Classification is:
 a set of rules defining inclusion
 a processor to apply rules
 a collection of nodes matching rules

TODO: How to apply inheritable. If each node kept an ordered set of parent_ids, this would be fast.
      With imcomplete data, constructing this is difficult.
      We could make a post construct method the fills in this list.
      Trying to traverse back up the tree gets tedious quickly.
"""
from collections import defaultdict
from pprint import pprint
from typing import Callable, Iterable, Dict, Set, List, Union

from customs import Customs, FileType
from node import Node


# classificatino rule example
#    def predicate(n: Node) -> bool:
def regex_property_match(n: Node) -> bool:
    pass


class Classifier:
    """
    All classifications are made by this single class - to avoid multiple collection traversals
    After classification, classed nodes are available by name (or label).
    
    Classifications may be specified by:
    - id
    - rule (predicate)
    """
    def __init__(self):
        self.result: Dict[str, Set] = defaultdict(set)
        self._id_rules: Dict[str, List[int]] = defaultdict(list)
        self._predicate_rules: Dict[str, List[Callable]] = defaultdict(list)

    def add_id(self, label: str, id: Union[int, List[int]]) -> None:
        if not isinstance(id, list):
            id = [id]
        self._id_rules[label].extend(id)
    
    def add_rule(self, label: str, predicate: Callable[[Node], bool]) -> None:
        """
        :param predicate - a function or lambda accepting a node and returning true if the
               node belongs to this classification.
               def is_specific_node(n: Node) -> bool:
                   return n.id in {id1, id2, ...}
        """
        self._predicate_rules[label].append(predicate)
    
    def classify(self, nodes: Dict[int, Node]) -> None:
        self.result = defaultdict(set)
        
        # Collect by id
        for k,v in self._id_rules.items():
            for id in v:
                self.result[k].add(nodes[id])
        
        # Collect by predicate
        for n in nodes.values():
            for label, predicates in self._predicate_rules.items():
                for predicate in predicates:
                    if predicate(n):
                        self.result[label].add(n)

    def print(self):
        for k,v in self.result.items():
            print(f"{k}:")
            for name in sorted([x.name for x in v]):
                print(f"    {name}")


c = Customs('case_100', FileType.PICKLE)
c.read()
c.translate()

cl = Classifier()
cl.add_id("specific", 9775968)
cl.add_id("specific", [9775967, 9775965])
cl.add_rule("py-files", lambda x: x.extension == "py")
cl.add_rule("big-files", lambda x: x.size > 10000)

cl.classify(c.id_dict)
cl.print()
