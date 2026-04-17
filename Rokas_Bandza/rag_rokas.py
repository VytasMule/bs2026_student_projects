#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path


def client_from_env():
    try:
        from elasticsearch import Elasticsearch
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'elasticsearch'. Install it with:\n"
            "pip install -r requirements.txt"
        ) from exc

    return Elasticsearch("http://localhost:9200")


def check_elasticsearch(es) -> None:
    try:
        if not es.ping():
            raise RuntimeError("Elasticsearch is not reachable at http://localhost:9200")
    except Exception as exc:
        raise RuntimeError(
            "Cannot connect to Elasticsearch at http://localhost:9200. "
            "Start Elasticsearch first."
        ) from exc


def ensure_index(es, index: str) -> None:
    if es.indices.exists(index=index):
        return
    es.indices.create(
        index=index,
        mappings={
            "properties": {
                "content": {"type": "text"},
                "path": {"type": "keyword"},
            }
        },
    )


def doc_count(es, index: str) -> int:
    if not es.indices.exists(index=index):
        return 0
    return int(es.count(index=index)["count"])


def clear_index(es, index: str) -> None:
    if not es.indices.exists(index=index):
        print(f"Index '{index}' does not exist yet.")
        return
    es.delete_by_query(
        index=index,
        query={"match_all": {}},
        conflicts="proceed",
        refresh=True,
    )
    print(f"Cleared index '{index}'.")


def split_paragraphs(text: str) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return paragraphs if paragraphs else [text.strip()]


def index_txt(es, index: str, folder: Path, clear: bool) -> None:
    if not folder.exists():
        raise FileNotFoundError(
            f"Folder not found: {folder}\n"
            f"Use a real folder path, for example:\n"
            f"python rag_rokas.py index --dir ../University_Project/DATA/DATA_TXT --clear"
        )

    ensure_index(es, index)

    if clear:
        es.delete_by_query(
            index=index,
            query={"match_all": {}},
            conflicts="proceed",
            refresh=True,
        )

    file_count = 0
    chunk_count = 0
    for file_path in sorted(folder.rglob("*.txt")):
        text = file_path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            continue

        file_count += 1
        for chunk_no, chunk in enumerate(split_paragraphs(text), start=1):
            if not chunk:
                continue
            doc_id = hashlib.sha1(f"{file_path}:{chunk_no}".encode("utf-8")).hexdigest()
            es.index(
                index=index,
                id=doc_id,
                document={
                    "content": chunk,
                    "path": str(file_path),
                    "chunk": chunk_no,
                },
            )
            chunk_count += 1

    es.indices.refresh(index=index)
    total = es.count(index=index)["count"]
    print(
        f"Indexed {file_count} txt files into {chunk_count} chunks. "
        f"Total docs in '{index}': {total}"
    )


def make_preview(text: str, query: str, width: int = 260) -> str:
    compact = " ".join(text.split())
    if not compact:
        return ""

    q_words = [w for w in re.findall(r"\w+", query.lower()) if len(w) >= 3]
    low = compact.lower()
    pos = -1
    for w in q_words:
        pos = low.find(w)
        if pos != -1:
            break

    if pos == -1:
        return compact[:300]

    start = max(0, pos - width // 2)
    end = min(len(compact), start + width)
    snippet = compact[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(compact):
        snippet = snippet + "..."
    return snippet


def ask(es, index: str, query: str, top_k: int) -> None:
    try:
        res = es.search(index=index, query={"match": {"content": {"query": query}}}, size=top_k)
    except Exception as exc:
        msg = str(exc)
        if "compatible-with=9" in msg:
            raise RuntimeError(
                "Elasticsearch Python client version is incompatible with your Elasticsearch server.\n"
                "Install v8 client:\n"
                "pip install 'elasticsearch>=8,<9'"
            ) from exc
        raise
    hits = res.get("hits", {}).get("hits", [])

    if not hits:
        print("No matching documents found.")
        return

    for i, hit in enumerate(hits, start=1):
        source = hit.get("_source", {})
        text = source.get("content", "")
        preview = make_preview(text, query)
        print(f"\n[{i}] score={hit.get('_score', 0):.3f}")
        print(f"path: {source.get('path', '<unknown>')} (chunk {source.get('chunk', '?')})")
        print(f"preview: {preview}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bare-bones TXT RAG (index + retrieve)")
    p.add_argument("--index", default="rokas_txt", help="Elasticsearch index name")

    sub = p.add_subparsers(dest="cmd")

    pi = sub.add_parser("index", help="Index .txt files")
    pi.add_argument("--dir", required=True, type=Path, help="Folder containing txt files")
    pi.add_argument("--clear", action="store_true", help="Clear index before indexing")

    pa = sub.add_parser("ask", help="Retrieve matching docs")
    pa.add_argument("question", help="Question/query text")
    pa.add_argument("--top-k", type=int, default=3)

    sub.add_parser("stats", help="Show document count")
    sub.add_parser("clear", help="Delete all docs in index")
    sub.add_parser("menu", help="Run interactive console menu")

    return p


def run_menu(es, index: str) -> None:
    while True:
        print("\nRAG Rokas Menu")
        print("1) Ask a question")
        print("2) Index data (.txt)")
        print("3) Show index stats")
        print("4) Clear index")
        print("5) Exit")

        choice = input("Choose (1-5): ").strip()

        try:
            if choice == "1":
                question = input("Question: ").strip()
                if not question:
                    print("Question cannot be empty.")
                    continue
                top_k_raw = input("Top-k [default 3]: ").strip()
                top_k = int(top_k_raw) if top_k_raw else 3
                ask(es, index, question, top_k)
            elif choice == "2":
                folder_raw = input("Folder path [default .]: ").strip()
                folder = Path(folder_raw) if folder_raw else Path(".")
                clear_raw = input("Clear index first? (y/N): ").strip().lower()
                index_txt(es, index, folder, clear=(clear_raw == "y"))
            elif choice == "3":
                total = doc_count(es, index)
                print(f"Index '{index}' contains {total} document(s).")
            elif choice == "4":
                confirm = input(f"Type 'clear' to wipe '{index}': ").strip().lower()
                if confirm == "clear":
                    clear_index(es, index)
                else:
                    print("Clear canceled.")
            elif choice == "5":
                print("Bye.")
                return
            else:
                print("Invalid option.")
        except Exception as exc:
            print(f"ERROR: {exc}")


def main() -> None:
    args = build_parser().parse_args()

    try:
        es = client_from_env()
        check_elasticsearch(es)

        if args.cmd in (None, "menu"):
            run_menu(es, args.index)
        elif args.cmd == "stats":
            total = doc_count(es, args.index)
            print(f"Index '{args.index}' contains {total} document(s).")
        elif args.cmd == "clear":
            clear_index(es, args.index)
        if args.cmd == "index":
            index_txt(es, args.index, args.dir, args.clear)
        elif args.cmd == "ask":
            ask(es, args.index, args.question, args.top_k)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
