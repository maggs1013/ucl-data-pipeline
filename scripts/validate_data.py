import os, pandas as pd

DATA_DIR = "data"

def head(df, n=5):
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(n))

def show_file(path, name):
    if not os.path.exists(path):
        print(f"[WARN] {name} not found at {path}")
        return None
    df = pd.read_csv(path)
    print(f"\n==== {name} ({len(df)} rows) ====")
    print("Columns:", list(df.columns))
    head(df, 5)
    return df

def check_required(df, cols, label):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"[WARN] {label}: missing columns → {missing}")
    else:
        print(f"[OK] {label}: all required columns present.")

def main():
    # Show enriched raw files (post-enrichment, pre-build) if useful:
    show_file(os.path.join(DATA_DIR, "raw_football_data.csv"), "raw_football_data.csv (enriched)")
    show_file(os.path.join(DATA_DIR, "raw_theodds_fixtures.csv"), "raw_theodds_fixtures.csv (enriched)")

    # Final model schema files
    hist = show_file(os.path.join(DATA_DIR, "HIST_matches.csv"), "HIST_matches.csv")
    upc  = show_file(os.path.join(DATA_DIR, "UPCOMING_fixtures.csv"), "UPCOMING_fixtures.csv")
    xg   = show_file(os.path.join(DATA_DIR, "xg_metrics.csv"), "xg_metrics.csv")

    # Required columns for model schema
    req_hist = ["date","home_team","away_team","home_goals","away_goals",
                "home_odds_dec","draw_odds_dec","away_odds_dec",
                "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
                "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
                "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    req_upc  = ["date","home_team","away_team",
                "home_odds_dec","draw_odds_dec","away_odds_dec",
                "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
                "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
                "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]

    if hist is not None: check_required(hist, req_hist, "HIST_matches.csv")
    if upc  is not None: check_required(upc,  req_upc,  "UPCOMING_fixtures.csv")

    # Check that xG was merged into raw files (optional visibility)
    if upc is not None:
        suspect_cols = ["home_xg","home_xga","home_xgd","home_xgd_per90",
                        "away_xg","away_xga","away_xgd","away_xgd_per90"]
        got = [c for c in suspect_cols if c in upc.columns]
        if got:
            print(f"[OK] Upcoming contains xG columns: {got}")
        else:
            print("[INFO] Upcoming has no xG columns (maybe leagues without adv stats or first run).")

if __name__ == "__main__":
    main()