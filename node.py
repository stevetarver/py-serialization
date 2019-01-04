import json
from pathlib import Path
from typing import List, NamedTuple, Optional, Tuple, Dict


class Node(NamedTuple):
    """
    inode metadata, created with Node.new()
    """
    id: int  # id first simplifies serialization
    tag: str  # TODO: this is probably better named kind
    name: str
    parent_id: Optional[int] # None for root node
    stem: str
    extension: str
    path: str
    size: int
    owner: int
    group: int
    created: int
    accessed: int
    modified: int
    owner_perm: int  # bits for owner inode perms: 4,2,1 (r,w,x)
    group_perm: int
    other_perm: int

    @staticmethod
    def new(path: Path) -> "Node":
        """
        Many ways to make a NamedTuple - all basically equal perf
            new_dict	    0.000053
            new_positional	0.000053
            make_dict	    0.000053
            make_list	    0.000058
            make_named	    0.000053
            make_positional	0.000053
        Note: Constructing Nodes from file info is limited to about 19,000 n/s
        """
        p = path.resolve(strict=True)
        stats = p.stat()
        data = {
            "tag": "Directory" if p.is_dir() else "File",
            "id": stats.st_ino,
            "parent_id": p.parent.stat().st_ino if p.parent else None,
            "name": p.name,
            "stem": p.stem,
            "extension": p.suffix[1:],  # omit the leading dot
            "path": f"{p.absolute()}",  # convert PosixPath to string
            "size": stats.st_size,
            "created": int(stats.st_ctime),
            "accessed": int(stats.st_atime),
            "modified": int(stats.st_mtime),
            "owner": stats.st_uid,
            "group": stats.st_gid,
            "owner_perm": int(oct(stats.st_mode)[-3]),
            "group_perm": int(oct(stats.st_mode)[-2]),
            "other_perm": int(oct(stats.st_mode)[-1]),
        }
        return Node(**data)

    def is_dir(self):
        return self.tag[0] == "D"

    # def bogus(self):
    #     print("bogus")

    def to_json(self):
        """
        For serialization. If we manually encode our json, we avoid the dict construction
        and encoding with the json codec.
        
        NOTES:
        - This improves write speed, not read speed.
        - Fragile - if data members change, need to keep this updated
        """
        # generation code
        # result = "f'{{"
        # for name, cls in Node._field_types.items():
        #     if cls == str:
        #         result += f'"{name}":"{{self.{name}}}",'
        #     else:
        #         result += f'"{name}":{{self.{name}}},'
        # result += "}}'"
        # print(result)
        return f'{{"id":{self.id},"tag":"{self.tag}","name":"{self.name}","parent_id":{self.parent_id},"stem":"{self.stem}","extension":"{self.extension}","path":"{self.path}","size":{self.size},"owner":{self.owner},"group":{self.group},"created":{self.created},"accessed":{self.accessed},"modified":{self.modified},"owner_perm":{self.owner_perm},"group_perm":{self.group_perm},"other_perm":{self.other_perm}}}'
    
    def __str__(self):
        return f"{self.name}   ({self.tag}: {self.path})"
    
    def __repr__(self):
        # Pretty cool, but Neo4j doesn't like the double quoted property names
        return f"({self.name} {json.dumps(self._asdict(), sort_keys=True, default=str)})"


class TreeNode(NamedTuple):
    me: Node
    files: List[Node]
    dirs: List["TreeNode"]
    
    def add(self, p: Path) -> Optional["TreeNode"]:
        """
        Add an item to the proper collection in this node
        :param p: Path object to add
        :return: a new TreeNode object if one was created (to hold a Dir Node), self unless exception
        """
        try:
            node = Node.new(p)
        except Exception as e:
            print(f"Exception in TreeNode.add: {e}")
        else:
            if node.is_dir():
                self.dirs.append(TreeNode(me=node, files=[], dirs=[]))
                return self.dirs[-1]
            else:
                self.files.append(node)
            return self
    
    @staticmethod
    def new(p: Path) -> "TreeNode":
        """ Create a new TreeNode - used only to create the root node, then use add() """
        return TreeNode(me=Node.new(p), files=[], dirs=[])

    def node_counts(self) -> Tuple[int, int]:
        """ Return the count of dirs, files in this hierarchy """
        # TODO this would be faster using the TreeNode.__iter__
        dirs = 0
        files = 0
        for item in self.node_iter():
            if item.is_dir():
                dirs += 1
            else:
                files += 1
        return dirs, files

    def node_iter(self) -> Node:
        """
        A recursive generator - producing nodes (stripping out the TreeNode part)
        
            for item in TreeNode.node_iter():
        """
        yield self.me
        for f in self.files:
            yield f
        for d in self.dirs:
            yield from d.node_iter()
    
    def print(self, indent=0) -> None:
        print(f"{' ' * indent}{self.me}")
        for f in self.files:
            print(f"{' ' * (indent + 2)}{f}")
        for d in self.dirs:
            d.print(indent + 2)  # recurse subtree

    def to_id_dict(self) -> Dict[int, Node]:
        """ Convert this TreeNode hierarchy to an inode keyed dict of Node """
        result = {}
        for item in self.node_iter():
            result[item.id] = item
        return result
    
    def to_tn_dict(self) -> Dict[int, "TreeNode"]:
        """ Convert this hierarchy to an inode -> TreeNode dict """
        result = {}
        for item in self.iter():
            result[item.me.id] = item
        return result

    # TODO: This is blowing up ./generator.py -d as __iter__; refine use for NT
    def iter(self):
        """
        Iterate over TreeNode objects in hierarchy
          for item in TreeNode.iter():
        """
        yield self
        for d in self.dirs:
            yield from d.iter()

    def __str__(self):
        return f"TreeNode {self.me.name} (files: {len(self.files)}, dirs: {len(self.dirs)}, path: {self.me.path})"
