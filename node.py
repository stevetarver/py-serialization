import json
from pathlib import Path
from typing import List, NamedTuple, Optional


class Node(NamedTuple):
    """
    inode metadata, created with Node.new()
    """
    id: int  # id first simplifies generating csv files
    tag: str  # TODO: in neo4j, I think this is better named a label - clean up, perhaps 'kind' is a better name
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
    owner_perm: int  # bits for owner inode perms: 1,2,4
    group_perm: int
    other_perm: int

    @staticmethod
    def new(path: Path) -> "Node":
        p = path.resolve(strict=True)
        stats = p.stat()
        data = {
            "tag": "Directory" if p.is_dir() else "File",
            "id": stats.st_ino,
            "parent_id": p.parent.stat().st_ino if p.parent else None,
            "name": p.name,
            "stem": p.stem,
            "extension": p.suffix[1:],  # omit the leading dot
            "path": f"{p.absolute()}",
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
        return self.tag.startswith("Dir")
    
    def __repr__(self):
        # Pretty cool, but Neo4j doesn't like the double quoted property names
        return f"({self.tag} {json.dumps(self._asdict(), sort_keys=True, default=str)})"


class TreeNode(NamedTuple):
    me: Node
    files: List[Node]
    dirs: List["TreeNode"]
    
    def add(self, p: Path) -> Optional["TreeNode"]:
        """
        Add an item to the proper collection in this node
        :param p: Path object to add
        :return: a TreeNode object if one was created (to hold a Dir Node)
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
            return node
    
    def new(p: Path) -> "TreeNode":
        """
        Create a new TreeNode - used only to create the root node, then use add()
        """
        return TreeNode(me=Node.new(p), files=[], dirs=[])
    
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
        for d in self.dirs:
            d.print(indent + 2)  # recurse subtree
        for f in self.files:
            print(f"{' ' * (indent + 2)}{f}")
