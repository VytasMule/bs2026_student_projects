# RAG Rokas - Super Easy Setup Guide (Clean Mac)

This guide is for a completely fresh Mac.
No VS Code needed. No coding experience needed.

## What this app does
- You give it `.txt` files
- It indexes them
- You ask questions in terminal menu

## 1) Install Python 3

1. Open Safari
2. Go to: https://www.python.org/downloads/macos/
3. Download latest Python 3 installer
4. Install it (just click Continue/Install)

Check it:
1. Open `Terminal` app
2. Run:
```bash
python3 --version
```
You should see something like `Python 3.x.x`.

## 2) Install Docker Desktop (for Elasticsearch)

1. Go to: https://www.docker.com/products/docker-desktop/
2. Download Docker Desktop for Mac
3. Install and open Docker Desktop
4. Wait until Docker says it is running

## 3) Get the RAG_Rokas folder

If someone sent you a zip:
1. Unzip it
2. Put folder somewhere easy, for example Desktop

You should have a folder named `RAG_Rokas`.

## 4) First Launch (double-click way)

1. Open `RAG_Rokas` folder in Finder
2. Double-click `Start_RAG_Rokas.command`
3. If macOS blocks it:
- Right-click file -> Open
- Click Open again

The launcher will:
- create Python environment
- install dependencies
- ask to start Elasticsearch with Docker
- open menu

## 5) Use the menu

You will see:
1. Ask a question
2. Index data (.txt)
3. Show index stats
4. Clear index
5. Exit

### First thing you should do: index files
1. Choose `2`
2. Folder path:
- type `.` if your txt files are inside `RAG_Rokas`
- or type full path, for example:
`/Users/rokas/Desktop/MyTexts`
3. `Clear index first?`
- type `y` if this is first run

Then choose `1` and ask questions.

## 6) How to add new files later

1. Put new `.txt` files into folder (or another folder)
2. Run `Start_RAG_Rokas.command`
3. Choose `2) Index data (.txt)`
4. Enter that folder path
5. Clear?
- `n` = keep old docs + add new
- `y` = remove old docs, keep only current set

## 7) Common problems and easy fixes

### "python3 not found"
Python is not installed. Go back to Step 1.

### "Elasticsearch is not running"
Open Docker Desktop and wait until it is fully running. Then start launcher again.

### Nothing found in answers
- You probably did not index files yet
- Or asked about topic not in your txt files
- Re-index with option `2`

### Wrong / old results
Re-index and choose clear = `y`.

## 8) Absolute shortest daily flow

1. Double-click `Start_RAG_Rokas.command`
2. Press `2` to index files (if files changed)
3. Press `1` to ask questions
4. Press `5` to exit

Done.
