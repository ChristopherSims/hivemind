"""Build a pinned developer-search corpus from explicitly allowlisted repository code."""
import argparse
import ast
import hashlib
import json
from pathlib import Path


def build_dataset(root, manifest):
    root = Path(root).resolve(strict=True)
    maximum = manifest["max_documents"]
    lines_limit = manifest["max_lines"]
    if (type(maximum) is not int or not 1 <= maximum <= 1000
            or type(lines_limit) is not int or not 4 <= lines_limit <= 200):
        raise ValueError("Invalid corpus bounds")
    documents, references = {}, {}
    for relative in manifest["sources"]:
        local = Path(relative)
        if (local.is_absolute() or ".." in local.parts or local.suffix != ".py"
                or local.parts[0] != "hivemind"):
            raise ValueError("Only explicitly allowlisted Hivemind Python sources are allowed")
        path = root / local
        if any(p.is_symlink() or getattr(p.lstat(), "st_file_attributes", 0) & 0x400
               for p in (path, *path.parents) if p.is_relative_to(root)):
            raise ValueError("Source links are not allowed")
        raw = path.read_bytes()
        if len(raw) > 1048576:
            raise ValueError("Source exceeds corpus bound")
        source = raw.decode("utf-8")
        lines = source.splitlines()
        tree = ast.parse(source)
        nodes = [(node.name, node) for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
        nodes += [(f"{node.name}.{method.name}", method) for node in tree.body
                  if isinstance(node, ast.ClassDef) for method in node.body
                  if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))]
        for name, node in nodes:
            if node.end_lineno - node.lineno < 3:
                continue
            key = local.as_posix() + "::" + name
            identity = "repo-" + hashlib.sha256(key.encode()).hexdigest()[:20]
            stop = min(node.end_lineno, node.lineno + lines_limit - 1)
            excerpt = "\n".join(lines[node.lineno - 1:stop])
            documents[key] = {
                "id": identity, "text": f"{key}\n{excerpt}",
                "source": {"path": local.as_posix(), "symbol": name, "start_line": node.lineno,
                           "end_line": stop, "function_end_line": node.end_lineno,
                           "file_sha256": hashlib.sha256(raw).hexdigest(),
                           "excerpt_sha256": hashlib.sha256(excerpt.encode()).hexdigest()}}
            references[key] = identity
    relevant = {key for query in manifest["queries"] for key in query["relevance"]}
    if not relevant <= documents.keys() or len(relevant) > maximum:
        raise ValueError("Missing or excessive judged symbols")
    # Freeze distractors deterministically, without consulting embeddings or results.
    extras = sorted(documents.keys() - relevant, key=lambda key: hashlib.sha256(key.encode()).hexdigest())
    selected = sorted(relevant | set(extras[:maximum - len(relevant)]))
    queries = [{**query, "relevance": {references[key]: grade for key, grade in query["relevance"].items()}}
               for query in manifest["queries"]]
    return {"version": manifest["version"], "label_policy": manifest["label_policy"],
            "corpus_scope": "Real repository code; assistant-authored developer questions and judgments. "
                            "Not independent labels, production traffic, board conversations or memory relevance.",
            "source_license": "Repository MIT license",
            "manifest_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            "documents": [documents[key] for key in selected], "queries": queries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Dataset snapshot already exists")
    dataset = build_dataset(args.root, json.loads(args.manifest.read_text(encoding="utf-8")))
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(json.dumps({"documents": len(dataset["documents"]), "queries": len(dataset["queries"])}))


if __name__ == "__main__":
    main()
