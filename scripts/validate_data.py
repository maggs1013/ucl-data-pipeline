import os, pandas as pd

DATA_DIR = "data"

def head(df, n=5):
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(n))

def show(path, label):
    if not os.path.exists(path):
        print(f"[WARN] {label} not found: {path}"); return None
    df = pd.read_csv(path)
    print(f"\n==== {label} ({len(df)} rows) ====")
    print("Columns:", list(df.columns)); head(df, 5); return df

def check_required(df, cols, label):
    miss = [c for c in cols if c not in df.columns]
    print(f"[{'OK' if not miss else 'WARN'}] {label} required cols: {('all present' if not miss else 'missing ' + str(miss))}")

def main():
    show(os.path.join(DATA_DIR, "raw_football_data.csv"), "raw_football_data.csv (enriched)")
    show(os.path.join(DATA_DIR, "raw_theodds_fixtures.csv"), "raw_theodds_fixtures.csv (enriched)")
    hist = show(os.path.join(DATA_DIR, "HIST_matches.csv"), "HIST_matches.csv")
    upc  = show(os.path.join(DATA_DIR, "UPCOMING_fixtures.csv"), "UPCOMING_fixtures.csv")
    show(os.path.join(DATA_DIR, "xg_metrics_current.csv"), "xg_metrics_current.csv")
    show(os.path.join(DATA_DIR, "xg_metrics_last.csv"), "xg_metrics_last.csv")
    show(os.path.join(DATA_DIR, "xg_metrics_hybrid.csv"), "xg_metrics_hybrid.csv")

    req_hist = ["date","home_team","away_team","home_goals","away_goals","home_odds_dec","draw_odds_dec","away_odds_dec",
                "home_rest_days","away_rest_days","home_travel_km","away_travel_km","home_injury_index","away_injury_index",
                "home_gk_rating","away_gk_rating","home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    req_upc  = ["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","home_rest_days","away_rest_days",
                "home_travel_km","away_travel_km","home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
                "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    if hist is not None: check_required(hist, req_hist, "HIST_matches.csv")
    if upc  is not None: check_required(upc,  req_upc,  "UPCOMING_fixtures.csv")

if __name__ == "__main__":
    main()
