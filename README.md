# CRUD Demo API (FastAPI + SQLite)

A simple CRUD (Create, Read, Update, Delete) REST API for an "Item" resource, built with FastAPI and SQLite.

## Endpoints

| Method | Path             | Description       |
|--------|------------------|--------------------|
| POST   | /items           | Create an item     |
| GET    | /items           | List all items     |
| GET    | /items/{id}      | Get one item       |
| PUT    | /items/{id}      | Update an item     |
| DELETE | /items/{id}      | Delete an item     |

Interactive docs are auto-generated at `/docs` (Swagger UI).

## Run locally

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Visit http://127.0.0.1:8000/docs to try it out.

## Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit: CRUD API with FastAPI and SQLite"
git branch -M main
git remote add origin https://github.com/<your-username>/<your-repo-name>.git
git push -u origin main
```

(Create the empty repo on GitHub first via "New repository" — don't initialize it with a README there, to avoid a merge conflict.)

## Deploy (free options)

SQLite + FastAPI can't be hosted on GitHub Pages (that's static-only). Use one of these instead:

**Render.com (recommended, free tier)**
1. Push code to GitHub (above).
2. Go to render.com → New → Web Service → connect your repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Deploy — Render gives you a live public URL.

**Railway.app** — similar flow, auto-detects Python and lets you set the same start command.

**Fly.io** — more control, needs `fly launch` + a `Dockerfile` (ask me if you want one).

Note: SQLite files reset on redeploy on most free hosts, since the filesystem isn't persistent. Fine for a demo/prototype; for production data, swap to a hosted Postgres (e.g. Render's free Postgres, or Supabase) — just change `DATABASE_URL` in `main.py`.
