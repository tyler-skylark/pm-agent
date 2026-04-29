#!/usr/bin/env python3
"""
Probe whether Basecamp will tolerate the parallel-fetch approach.

Tests three patterns matching what the optimized agent.py would do:
  A. Sequential baseline (current behavior)
  B. Parallel todolist fetches inside one project (proposed #2)
  C. Parallel project fetches (proposed #1)

Watches for 429s, timing, and reports the per-request headers Basecamp
returns so we can size max_workers safely.
"""

import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import certifi
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# Patch agent.py's urllib calls to use certifi (Mac python.org has no CA bundle).
_orig_urlopen = urllib.request.urlopen
def _patched_urlopen(req, *args, **kwargs):
    kwargs.setdefault("context", SSL_CTX)
    return _orig_urlopen(req, *args, **kwargs)
urllib.request.urlopen = _patched_urlopen

from agent import (
    BC_BASE,
    USER_AGENT,
    bc_get_all,
    fetch_active_sky_projects,
    load_env,
    load_secrets_from_gcp,
    refresh_bc_token,
    token_needs_refresh,
)


def raw_get(path, params=None):
    """Single GET — returns (status, headers, body, elapsed_ms)."""
    url = path if path.startswith("http") else f"{BC_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {os.environ['BC_ACCESS_TOKEN']}",
        "User-Agent": USER_AGENT,
    })
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15, context=SSL_CTX) as resp:
            body_len = len(resp.read())
            return resp.status, dict(resp.headers), body_len, (time.perf_counter() - t0) * 1000
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), 0, (time.perf_counter() - t0) * 1000
    except Exception as e:
        return None, {"error": str(e)}, 0, (time.perf_counter() - t0) * 1000


def show_rate_headers(headers, label):
    """Pull any rate-limit-ish headers Basecamp returns."""
    interesting = {k: v for k, v in headers.items() if "rate" in k.lower() or "retry" in k.lower()}
    if interesting:
        print(f"  {label} rate headers: {interesting}")


def test_a_sequential(todolist_ids, proj_id):
    print("\n=== Test A: Sequential baseline (10 todolists) ===")
    t0 = time.perf_counter()
    statuses = []
    for i, lid in enumerate(todolist_ids[:10]):
        s, h, _, ms = raw_get(f"/buckets/{proj_id}/todolists/{lid}/todos.json", {"completed": "false"})
        statuses.append(s)
        print(f"  [{i+1:2d}] {s}  {ms:.0f}ms")
        if i == 0:
            show_rate_headers(h, "first req")
    total = (time.perf_counter() - t0) * 1000
    print(f"  TOTAL: {total:.0f}ms across {len(statuses)} reqs ({total/len(statuses):.0f}ms avg)")
    print(f"  429s: {sum(1 for s in statuses if s == 429)} / non-200: {sum(1 for s in statuses if s != 200)}")
    return total, statuses


def test_b_parallel_lists(todolist_ids, proj_id, workers):
    print(f"\n=== Test B: Parallel todolists, workers={workers} (10 todolists) ===")
    def fetch(lid):
        s, _, _, ms = raw_get(f"/buckets/{proj_id}/todolists/{lid}/todos.json", {"completed": "false"})
        return lid, s, ms
    t0 = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for f in as_completed([pool.submit(fetch, lid) for lid in todolist_ids[:10]]):
            results.append(f.result())
    total = (time.perf_counter() - t0) * 1000
    statuses = [r[1] for r in results]
    print(f"  TOTAL: {total:.0f}ms across {len(results)} reqs (parallel)")
    print(f"  429s: {sum(1 for s in statuses if s == 429)} / non-200: {sum(1 for s in statuses if s != 200)}")
    print(f"  per-req latencies: min={min(r[2] for r in results):.0f}ms max={max(r[2] for r in results):.0f}ms")
    return total, statuses


def test_c_parallel_projects(projects, workers):
    print(f"\n=== Test C: Parallel projects, workers={workers} (top 8 projects, /projects/{{id}}.json each) ===")
    sample = projects[:8]
    def fetch(pid):
        s, _, _, ms = raw_get(f"/projects/{pid}.json")
        return pid, s, ms
    t0 = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for f in as_completed([pool.submit(fetch, p["id"]) for p in sample]):
            results.append(f.result())
    total = (time.perf_counter() - t0) * 1000
    statuses = [r[1] for r in results]
    print(f"  TOTAL: {total:.0f}ms across {len(results)} reqs (parallel)")
    print(f"  429s: {sum(1 for s in statuses if s == 429)} / non-200: {sum(1 for s in statuses if s != 200)}")
    return total, statuses


def test_d_burst(projects, n=30, workers=8):
    """Realistic worst-case — what a fully parallelized briefing might burst."""
    print(f"\n=== Test D: Burst stress, {n} reqs / workers={workers} ===")
    sample = (projects * ((n // len(projects)) + 1))[:n]
    def fetch(p):
        s, h, _, ms = raw_get(f"/projects/{p['id']}.json")
        return p["id"], s, ms, h
    t0 = time.perf_counter()
    statuses = []
    last_headers = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for f in as_completed([pool.submit(fetch, p) for p in sample]):
            pid, s, ms, h = f.result()
            statuses.append(s)
            last_headers = h
    total = (time.perf_counter() - t0) * 1000
    print(f"  TOTAL: {total:.0f}ms across {n} reqs")
    print(f"  429s: {sum(1 for s in statuses if s == 429)} / non-200: {sum(1 for s in statuses if s != 200)}")
    print(f"  effective rate: {n / (total/1000):.1f} req/s")
    show_rate_headers(last_headers, "last req")


def main():
    load_env()
    load_secrets_from_gcp()
    if token_needs_refresh():
        refresh_bc_token()

    print("Fetching project list...")
    projects = bc_get_all("/projects.json") or []
    sky = fetch_active_sky_projects(projects)
    print(f"Found {len(sky)} active SKY projects")

    # Pick a project with multiple todosets/todolists for tests A/B
    pick = None
    todolist_ids = []
    for p in sky:
        todoset_tools = [d for d in p.get("dock", []) if d.get("name") == "todoset" and d.get("enabled")]
        if not todoset_tools:
            continue
        all_lists = []
        for ts in todoset_tools:
            tl = bc_get_all(f"/buckets/{p['id']}/todosets/{ts['id']}/todolists.json")
            all_lists.extend(tl or [])
        if len(all_lists) >= 10:
            pick = p
            todolist_ids = [t["id"] for t in all_lists]
            print(f"Using {p['name']} for todolist tests ({len(all_lists)} lists)")
            break
    if not pick:
        print("No project with >=10 todolists found; falling back to first project")
        pick = sky[0]

    test_a_sequential(todolist_ids, pick["id"])
    time.sleep(2)  # let any rate window reset
    test_b_parallel_lists(todolist_ids, pick["id"], workers=4)
    time.sleep(2)
    test_b_parallel_lists(todolist_ids, pick["id"], workers=8)
    time.sleep(2)
    test_c_parallel_projects(sky, workers=8)
    time.sleep(2)
    test_d_burst(sky, n=30, workers=8)
    time.sleep(2)
    test_d_burst(sky, n=50, workers=8)

    print("\nDone.")


if __name__ == "__main__":
    main()
