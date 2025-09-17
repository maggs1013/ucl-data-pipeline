# scripts/enrich_features.py
# Robust enrichment:
# - Fills missing columns with sensible defaults
# - Merges team defaults (GK, set-piece, crowd)
# - Applies injuries & lineups if present
# - Applies ref baselines if ref_name present
# - Computes travel_km if stadium lat/lon available
# - Never KeyErrors on missing columns

import os, math
import pandas as pd

DATA_DIR = "data"

# ======= utilities =======

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(a))

def safe_read(path):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

def ensure_cols(df, cols_with_defaults):
    """Create any missing columns with default values."""
    for col, default in cols_with_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df

def merge_team_master(df, teams):
    """Bring in team defaults and fill GK/set-piece/crowd if missing."""
    if teams.empty:
        # ensure the columns exist anyway
        return ensure_cols(df, {
            "home_gk_rating": 0.6, "away_gk_rating": 0.6,
            "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
            "crowd_index": 0.7
        })

    # prefix-merge defaults
    df = df.merge(teams.add_prefix("home_"), left_on="home_team", right_on="home_team", how="left")
    df = df.merge(teams.add_prefix("away_"), left_on="away_team", right_on="away_team", how="left")

    # If no crowd_index present, copy from home team default; else keep existing
    if "crowd_index" not in df.columns or df["crowd_index"].isna().all():
        if "home_crowd_index" in df.columns:
            df["crowd_index"] = df["home_crowd_index"]
        else:
            df["crowd_index"] = 0.7

    # Ensure GK & set-piece columns exist, then fill from team defaults if missing
    df = ensure_cols(df, {
        "home_gk_rating": None, "away_gk_rating": None,
        "home_setpiece_rating": None, "away_setpiece_rating": None
    })

    if "home_gk_rating_x" in df.columns:  # unlikely, but avoid clashes
        df.rename(columns={"home_gk_rating_x": "home_gk_rating"}, inplace=True)
    if "away_gk_rating_x" in df.columns:
        df.rename(columns={"away_gk_rating_x": "away_gk_rating"}, inplace=True)

    # fillna from team defaults
    if "home_gk_rating" in df.columns and "home_gk_rating_y" in df.columns:
        df["home_gk_rating"] = df["home_gk_rating"].fillna(df["home_gk_rating_y"])
    if "away_gk_rating" in df.columns and "away_gk_rating_y" in df.columns:
        df["away_gk_rating"] = df["away_gk_rating"].fillna(df["away_gk_rating_y"])

    if "home_setpiece_rating" in df.columns and "home_setpiece_rating_y" in df.columns:
        df["home_setpiece_rating"] = df["home_setpiece_rating"].fillna(df["home_setpiece_rating_y"])
    if "away_setpiece_rating" in df.columns and "away_setpiece_rating_y" in df.columns:
        df["away_setpiece_rating"] = df["away_setpiece_rating"].fillna(df["away_setpiece_rating_y"])

    # If still NaN after merging, hard defaults
    df = df.fillna({
        "home_gk_rating": 0.6, "away_gk_rating": 0.6,
        "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
        "crowd_index": 0.7
    })

    # drop the extra merged columns if present
    for c in list(df.columns):
        if c.endswith("_x") or c.endswith("_y"):
            # keep main columns only
            pass
    # Not strictly necessary to drop; leaving as-is is fine.

    return df

def apply_ref_rates(df, refs):
    if not refs.empty and "ref_name" in df.columns:
        df = df.merge(refs, how="left", on="ref_name")
        if "ref_pen_rate_y" in df.columns:
            df["ref_pen_rate"] = df.get("ref_pen_rate_x", df.get("ref_pen_rate", 0.30)).fillna(df["ref_pen_rate_y"])
        else:
            # if merge didn't bring a y-column, make sure we have ref_pen_rate
            df["ref_pen_rate"] = df.get("ref_pen_rate", 0.30).fillna(0.30)
        # cleanup
        for c in ("ref_pen_rate_x","ref_pen_rate_y"):
            if c in df.columns:
                df.drop(columns=[c], inplace=True)
    else:
        df["ref_pen_rate"] = df.get("ref_pen_rate", 0.30).fillna(0.30)
    return df

def apply_injuries(df, inj):
    if inj.empty:
        return df
    inj["date"] = pd.to_datetime(inj["date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # Ensure injury cols exist
    df = ensure_cols(df, {"home_injury_index": None, "away_injury_index": None})
    df = df.merge(inj.rename(columns={"team":"home_team","injury_index":"home_injury_index"}),
                  on=["date","home_team"], how="left")
    df = df.merge(inj.rename(columns={"team":"away_team","injury_index":"away_injury_index"]),
                  on=["date","away_team"], how="left")
    # fill missing with 0.3 default (mild)
    df["home_injury_index"] = df["home_injury_index"].fillna(0.3)
    df["away_injury_index"] = df["away_injury_index"].fillna(0.3)
    return df

def apply_lineup_flags(df, lu):
    if lu.empty:
        return df
    lu["date"] = pd.to_datetime(lu["date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for side in ["home","away"]:
        m = lu.rename(columns={
            "team":f"{side}_team",
            "key_att_out":f"{side}_key_att_out",
            "key_def_out":f"{side}_key_def_out",
            "keeper_changed":f"{side}_keeper_changed"
        })
        df = df.merge(m, on=["date",f"{side}_team"], how="left")
        # If missing, default to 0
        for flag in [f"{side}_key_att_out", f"{side}_key_def_out", f"{side}_keeper_changed"]:
            df[flag] = df.get(flag, 0).fillna(0).astype(int)
    return df

def compute_travel(df, stad):
    # Ensure travel columns exist
    df = ensure_cols(df, {"home_travel_km": None, "away_travel_km": None})
    if stad.empty:
        # Defaults if stadium data not present
        df["home_travel_km"] = df["home_travel_km"].fillna(0.0)
        df["away_travel_km"] = df["away_travel_km"].fillna(200.0)
        return df

    sH = stad.add_prefix("home_")
    sA = stad.add_prefix("away_")
    df = df.merge(sH, how="left", left_on="home_team", right_on="home_team")
    df = df.merge(sA, how="left", left_on="away_team", right_on="away_team")

    def row_dist(r):
        if pd.isna(r.get("home_lat")) or pd.isna(r.get("away_lat")):
            return 200.0
        return haversine(r["home_lat"], r["home_lon"], r["away_lat"], r["away_lon"])

    # Home travel ~ 0 if missing
    df["home_travel_km"] = df["home_travel_km"].fillna(0.0)
    # Away travel: compute for missing
    mask = df["away_travel_km"].isna()
    if mask.any():
        df.loc[mask, "away_travel_km"] = df[mask].apply(row_dist, axis=1)

    # Clean up merged cols if desired (not required)
    return df

def normalize_dates(df):
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    return df

def enrich_file(path, teams, stad, refs, inj, lu):
    if not os.path.exists(path):
        return
    df = pd.read_csv(path)

    # Ensure absolutely required id columns exist
    df = ensure_cols(df, {
        "home_team": "", "away_team": "",
        "home_odds_dec": None, "draw_odds_dec": None, "away_odds_dec": None,
    })

    df = normalize_dates(df)

    # Fill defaults for context columns so downstream never breaks
    df = ensure_cols(df, {
        "home_rest_days": 4, "away_rest_days": 4,
        "home_injury_index": 0.3, "away_injury_index": 0.3,
        "home_gk_rating": 0.6, "away_gk_rating": 0.6,
        "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
        "ref_pen_rate": 0.30, "crowd_index": 0.7,
        "home_travel_km": None, "away_travel_km": None,
    })

    # Merge in team defaults, injuries, lineups, refs, and travel
    df = merge_team_master(df, teams)
    df = apply_injuries(df, inj)
    df = apply_lineup_flags(df, lu)
    df = apply_ref_rates(df, refs)
    df = compute_travel(df, stad)

    # Final safety fills
    df = df.fillna({
        "home_rest_days": 4, "away_rest_days": 4,
        "home_injury_index": 0.3, "away_injury_index": 0.3,
        "home_gk_rating": 0.6, "away_gk_rating": 0.6,
        "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
        "ref_pen_rate": 0.30, "crowd_index": 0.7,
        "home_travel_km": 0.0, "away_travel_km": 200.0
    })

    df.to_csv(path, index=False)
    print(f"Enriched {path} with {len(df)} rows")

def main():
    teams = safe_read(os.path.join(DATA_DIR, "teams_master.csv"))
    stad  = safe_read(os.path.join(DATA_DIR, "stadiums.csv"))
    refs  = safe_read(os.path.join(DATA_DIR, "ref_baselines.csv"))
    inj   = safe_read(os.path.join(DATA_DIR, "injuries.csv"))      # optional
    lu    = safe_read(os.path.join(DATA_DIR, "lineups.csv"))       # optional

    # Enrich both historical and upcoming if present
    enrich_file(os.path.join(DATA_DIR, "raw_football_data.csv"), teams, stad, refs, inj, lu)
    enrich_file(os.path.join(DATA_DIR, "raw_theodds_fixtures.csv"), teams, stad, refs, inj, lu)

    print("Enrichment complete.")

if __name__ == "__main__":
    main()
