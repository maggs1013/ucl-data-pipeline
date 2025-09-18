# scripts/fetch_fbr_xg.py
# Fetch team xG/xGA/xGD standings from FBR API (FBref-sourced)
# Writes data/xg_metrics.csv
# Requires: GitHub secret FBR_API_KEY

import os, sys, time, requests, pandas as pd

OUT = "data/xg_metrics.csv"
API_KEY = os.environ.get("FBR_API_KEY", "").strip()
BASE = "https://fbrapi.com"

# League IDs to fetch (you can add more later)
LEAGUE_IDS = [
    9,   # EPL
    12,  # LaLiga
    11,  # Serie A
    20,  # Bundesliga
    13,  # Ligue 1
]

def to_df(rows):
    if not rows:
        return pd.DataFrame(columns=["league_id","team","xg","xga","xgd","xgd_per90","season"])
    return pd.DataFrame(rows)

def fetch_league_xg(league_id):
    url = f"{BASE}/league-standings"
    h = {"X-API-Key": API_KEY}
    r = requests.get(url, params={"league_id": league_id}, headers=h, timeout=30)
    if r.status_code != 200:
        print(f"[WARN] league_id={league_id} -> {r.status_code} {r.text[:120]}")
        return []
    js = r.json()
    data = js.get("data", js)
    rows = []
    for row in data:
        team = row.get("team") or row.get("team_name")
        if not team:
            continue
        rows.append({
            "league_id": league_id,
            "team": team,
            "xg": row.get("xg"),
            "xga": row.get("xga"),
            "xgd": row.get("xgd"),
            "xgd_per90": row.get("xgd_per90") or row.get("xgd_90"),
            "season": row.get("season") or row.get("year"),
        })
    return rows

def main():
    os.makedirs("data", exist_ok=True)
    if not API_KEY:
        print("[INFO] No FBR_API_KEY set. Writing empty xg_metrics.csv.")
        to_df([]).to_csv(OUT, index=False)
        sys.exit(0)

    all_rows = []
    for lid in LEAGUE_IDS:
        all_rows.extend(fetch_league_xg(lid))
        time.sleep(3.2)  # respect ~1 request/3s

    df = to_df(all_rows)
    df["team"] = df["team"].astype(str).str.replace(r"\s+\(.*\)$", "", regex=True).str.strip()
    df.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} with {len(df)} rows")

if __name__ == "__main__":
    main()
