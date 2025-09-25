from sqlalchemy.orm import Session
from models.model import LOC_ORG_HIERARCHY


def resolve_permissions_with_inheritance(session: Session, permissions: set):
    all_nodes = session.query(LOC_ORG_HIERARCHY).all()
    node_map = {(n.ORG_CODE, n.ORG_VALUE): n for n in all_nodes}
    descendants = set()

    def add_descendants(key):
        stack = [key]
        while stack:
            current = stack.pop()
            if current not in descendants:
                descendants.add(current)
                current_node = node_map.get(current)
                if current_node:
                    children = [
                        (n.ORG_CODE, n.ORG_VALUE)
                        for n in all_nodes
                        if n.PARENT_CODE == current_node.ORG_CODE and n.PARENT_VALUE == current_node.ORG_VALUE
                    ]
                    stack.extend(children)

    for perm in permissions:
        if perm in node_map:
            add_descendants(perm)

    return descendants
