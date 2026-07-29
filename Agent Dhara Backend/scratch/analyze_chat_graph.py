import ast
import os

here = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.dirname(here)
chat_graph_path = os.path.join(workspace_root, "agent", "chat_graph.py")

with open(chat_graph_path, "r", encoding="utf-8") as f:
    source = f.read()

tree = ast.parse(source)
for node in tree.body:
    name = ""
    if isinstance(node, ast.FunctionDef):
        name = f"Function: {node.name}"
    elif isinstance(node, ast.ClassDef):
        name = f"Class: {node.name}"
    elif isinstance(node, (ast.Assign, ast.AnnAssign)):
        # Get target names
        targets = []
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    targets.append(t.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                targets.append(node.target.id)
        name = f"Assignment: {', '.join(targets)}"
    elif isinstance(node, (ast.Import, ast.ImportFrom)):
        name = "Import"
    else:
        name = type(node).__name__
    
    print(f"{node.lineno}-{node.end_lineno}: {name}")
