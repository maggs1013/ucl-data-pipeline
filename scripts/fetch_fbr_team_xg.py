# scripts/fetch_fbr_team_xg.py
# Fetch team xG/xGA/xGD from FBR API for current + last season, build hybrid (60% current, 40% last)
# Writes:
#   data/xg_metrics_current.csv
#   data/xg_metrics_last.csv
#   data/xg_metrics_hybrid.csv

import os, sys, time, requests, pandas as pd

API_KEY = os.environ.get("FBR_API_KEY", "").strip()
BASE = "https://fbrapi.com"
DATA_DIR = "data"

# Big-5 leagues (FBref IDs). Add more if you want:
LEAGUE_IDS = [9, 12, 11, 20, 13]  # EPL, LaLiga, Serie A, Bundesliga, Ligue 1

def get(path, params=None):
    h = {"X-API-Key": API_KEY} if API_KEY else {}
    r = requests.get(f"{BASE}{path}", params=params or {}, headers=h, timeout=30)
    if r.status_code != 200:
        print(f"[WARN] GET {path} {params} -> {r.status_code} {r.text[:200]}")
        return None
    try:
        return r.json()
    except Exception as e:
        print("[WARN] JSON error:", e, r.text[:200]); return None

def list_seasons_for_league(league_id):
    js = get("/league-seasons", {"league_id": league_id})
    if not js: return []
    data = js.get("data", js)
    try: data = sorted(data, key=lambda x: x.get("season_id", 0))
    except: pass
    return data

def fetch_standings_xg(league_id, season_id=None):
    params = {"league_id": league_id}
    if season_id: params["season_id"] = season_id
    js = get("/league-standings", params)
    if not js: return []
    data = js.get("data", js)
    rows = []
    for row in data:
        team = row.get("team") or row.get("team_name")
        if not team: continue
        rows.append({
            "league_id": league_id,
            "season_id": season_id,
            "season": row.get("season") or row.get("year"),
            "team": str(team).strip(),
            "xg": row.get("xg"),
            "xga": row.get("xga"),
            "xgd": row.get("xgd"),
            "xgd_per90": row.get("xgd_per90") or row.get("xgd_90"),
        })
    return rows

def to_df(rows, cols):
    if not rows: return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    df["team"] = df["team"].str.replace(r"\s+\(.*\)$", "", regex=True).str.strip()
    return df

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not API_KEY:
        print("[INFO] No FBR_API_KEY set. Writing empty xg files and exiting.")
        empty_cols = ["league_id","season_id","season","team","xg","xga","xgd","xgd_per90"]
        pd.DataFrame(columns=empty_cols).to_csv(os.path.join(DATA_DIR, "xg_metrics_current.csv"), index=False)
        pd.DataFrame(columns=empty_cols).to_csv(os.path.join(DATA_DIR, "xg_metrics_last.csv"), index=False)
        pd.DataFrame(columns=["team","xg_hybrid","xga_hybrid","xgd_hybrid","xgd90_hybrid","league_id"]).to_csv(
            os.path.join(DATA_DIR, "xg_metrics_hybrid.csv"), index=False)
        sys.exit(0)

    cols = ["league_id","season_id","season","team","xg","xga","xgd","xgd_per90"]
    cur_rows, last_rows = [], []

    for lid in LEAGUE_IDS:
        seasons = list_seasons_for_league(lid)
        if not seasons:
            print(f"[WARN] No seasons for league_id={lid}"); continue

        current = seasons[-1]
        previous = seasons[-2] if len(seasons) >= 2 else None

        cur_rows.extend(fetch_standings_xg(lid, current.get("season_id"))); time.sleep(3.2)
        if previous:
            last_rows.extend(fetch_standings_xg(lid, previous.get("season_id"))); time.sleep(3.2)

    df_cur = to_df(cur_rows, cols)
    df_last = to_df(last_rows, cols)
    df_cur.to_csv(os.path.join(DATA_DIR, "xg_metrics_current.csv"), index=False)
    df_last.to_csv(os.path.join(DATA_DIR, "xg_metrics_last.csv"), index=False)
    print(f"[OK] wrote data/xg_metrics_current.csv ({len(df_cur)}) and data/xg_metrics_last.csv ({len(df_last)})")

    def sel(df, prefix):
        return df.rename(columns={
            "xg": f"{prefix}_xg",
            "xga": f"{prefix}_xga",
            "xgd": f"{prefix}_xgd",
            "xgd_per90": f"{prefix}_xgd90",
        })[["team","league_id",f"{prefix}_xg",f"{prefix}_xga",f"{prefix}_xgd",f"{prefix}_xgd90"]]

    hybrid = sel(df_cur, "cur") if not df_cur.empty else pd.DataFrame(columns=["team","league_id","cur_xg","cur_xga","cur_xgd","cur_xgd90"])
    if not df_last.empty:
        hybrid = hybrid.merge(sel(df_last, "last"), on=["team","league_id"], how="outer")
    else:
        for c in ["last_xg","last_xga","last_xgd","last_xgd90"]: hybrid[c] = None

    for c in ["cur_xg","cur_xga","cur_xgd","cur_xgd90","last_xg","last_xga","last_xgd","last_xgd90"]:
        hybrid[c] = pd.to_numeric(hybrid[c], errors="coerce")

    w_cur, w_last = 0.60, 0.40
    def w(a,b):
        if pd.notna(a) and pd.notna(b): return w_cur*a + w_last*b
        if pd.notna(a): return a
        if pd.notna(b): return b
        return None

    hybrid["xg_hybrid"]    = [w(a,b) for a,b in zip(hybrid["cur_xg"],   hybrid["last_xg"])]
    hybrid["xga_hybrid"]   = [w(a,b) for a,b in zip(hybrid["cur_xga"],  hybrid["last_xga"])]
    hybrid["xgd_hybrid"]   = [w(a,b) for a,b in zip(hybrid["cur_xgd"],  hybrid["last_xgd"])]
    hybrid["xgd90_hybrid"] = [w(a,b) for a,b in zip(hybrid["cur_xgd90"],hybrid["last_xgd90"])]

    out = hybrid[["team","league_id","xg_hybrid","xga_hybrid","xgd_hybrid","xgd90_hybrid"]].copy()
    out.to_csv(os.path.join(DATA_DIR, "xg_metrics_hybrid.csv"), index=False)
    print(f"[OK] wrote data/xg_metrics_hybrid.csv ({len(out)})")

if __name__ == "__main__":
    main()
