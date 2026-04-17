# RAG_Rokas (Bare Bones)

Need a very simple non-technical guide?
Open: `ROKAS_EASY_GUIDE.md`

Minimal TXT-only RAG:
- Index `.txt` files into Elasticsearch
- Retrieve matching docs for a question
- Indexes by paragraph chunks for better match quality
- No UI, no generation, no voice

## Setup

Fastest demo start (double-click on macOS):
- Open this folder in Finder
- Double-click `Start_RAG_Rokas.command`
- On first run it installs dependencies and can start Elasticsearch with Docker

Manual setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Make sure Elasticsearch is running on `http://localhost:9200`.

## Usage

Interactive menu (recommended):
```bash
python3 rag_rokas.py
```
Or simply double-click `Start_RAG_Rokas.command`.
Menu options:
- `1` Ask a question
- `2` Index data (`.txt`)
- `3` Show index stats
- `4` Clear index
- `5` Exit

Index txt files:
```bash
python3 rag_rokas.py index --dir ../University_Project/DATA/DATA_TXT --clear
```
Re-run indexing with `--clear` after code updates so old full-file docs are replaced by chunked docs.

Ask a question:
```bash
python3 rag_rokas.py ask "gravityx roadmap" --top-k 3
```

Optional custom index name:
```bash
python3 rag_rokas.py --index my_index index --dir ../University_Project/DATA/DATA_TXT
python3 rag_rokas.py --index my_index ask "question"
```

Show stats / clear index:
```bash
python3 rag_rokas.py stats
python3 rag_rokas.py clear
```

If you previously installed `elasticsearch` v9, reinstall compatible v8 client:
```bash
pip install --upgrade \"elasticsearch>=8,<9\"
```
