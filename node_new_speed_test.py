#!/usr/bin/env python3
"""
Speed test the various ways of making a Node (or just a Namedtuple
"""
from pathlib import Path

from node import Node
from timeit import default_timer as timer


def new_dict(path: Path) -> "Node":
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


def new_positional(path: Path) -> Node:
    p = path.resolve(strict=True)
    stats = p.stat()
    return Node(
        stats.st_ino,
        "Directory" if p.is_dir() else "File",
        p.name,
        p.parent.stat().st_ino if p.parent else None,
        p.stem,
        p.suffix[1:],
        f"{p.absolute()}",
        stats.st_size,
        stats.st_uid,
        stats.st_gid,
        int(stats.st_ctime),
        int(stats.st_atime),
        int(stats.st_mtime),
        int(oct(stats.st_mode)[-3]),
        int(oct(stats.st_mode)[-2]),
        int(oct(stats.st_mode)[-1]),
    )


def make_dict(path: Path) -> Node:
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
    return Node._make(**data)


def make_list(path: Path) -> Node:
    p = path.resolve(strict=True)
    stats = p.stat()
    return Node._make([
        stats.st_ino,
        "Directory" if p.is_dir() else "File",
        p.name,
        p.parent.stat().st_ino if p.parent else None,
        p.stem,
        p.suffix[1:],
        f"{p.absolute()}",
        stats.st_size,
        stats.st_uid,
        stats.st_gid,
        int(stats.st_ctime),
        int(stats.st_atime),
        int(stats.st_mtime),
        int(oct(stats.st_mode)[-3]),
        int(oct(stats.st_mode)[-2]),
        int(oct(stats.st_mode)[-1]),
    ])


def make_named(path: Path) -> Node:
    p = path.resolve(strict=True)
    stats = p.stat()
    return Node._make(
        id=stats.st_ino,
        tag="Directory" if p.is_dir() else "File",
        name=p.name,
        parent_id=p.parent.stat().st_ino if p.parent else None,
        stem=p.stem,
        extension=p.suffix[1:],
        path=f"{p.absolute()}",
        size=stats.st_size,
        owner=stats.st_uid,
        group=stats.st_gid,
        created=int(stats.st_ctime),
        accessed=int(stats.st_atime),
        modified=int(stats.st_mtime),
        owner_perm=int(oct(stats.st_mode)[-3]),
        group_perm=int(oct(stats.st_mode)[-2]),
        other_perm=int(oct(stats.st_mode)[-1]),
    )


def make_positional(path: Path) -> Node:
    p = path.resolve(strict=True)
    stats = p.stat()
    return Node._make(
        stats.st_ino,
        "Directory" if p.is_dir() else "File",
        p.name,
        p.parent.stat().st_ino if p.parent else None,
        p.stem,
        p.suffix[1:],
        f"{p.absolute()}",
        stats.st_size,
        stats.st_uid,
        stats.st_gid,
        int(stats.st_ctime),
        int(stats.st_atime),
        int(stats.st_mtime),
        int(oct(stats.st_mode)[-3]),
        int(oct(stats.st_mode)[-2]),
        int(oct(stats.st_mode)[-1]),
    )


def main():
    """ Compare creating new nodes by _make and dict construct """
    funcs = {
        "new_dict": new_dict,
        "new_positional": new_positional,
        "make_dict": make_dict,
        "make_list": make_list,
        "make_named": make_named,
        "make_positional": make_positional,
    }
    iterations = 10000
    path = Path("/Users/starver")
    ref_node = new_dict(path)
    
    for name, func in funcs.items():
        duration = 0
        for _ in range(iterations):
            start = timer()
            n = Node.new(path)
            duration += timer() - start
            # set iterations=1 and uncomment for validation
            # assert ref_node == n
            # print(n.__repr__())
        print(f"{name}\t{duration / iterations:.6f}")


if __name__ == "__main__":
    main()
