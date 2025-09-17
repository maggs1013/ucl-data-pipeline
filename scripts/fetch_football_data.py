import os, pandas as pd
from utils import download_csv

OUT_HIST = "data/raw_football_data.csv"
os.makedirs("data", exist_ok=True)

# NOTE: CL.csv (UCL) is often published later. Use top-5 leagues now; add CL later when available.
# 24/25 season (2425):
URLS_2425 = [
    "https://www.football-data.co.uk/mmz4281/2425/E0.csv",  # Premier League
    "https://www.football-data.co.uk/mmz4281/2425/D1.csv",  # Bundesliga
    "https://www.football-data.co.uk/mmz4281/2425/I1.csv",  # Serie A
    "https://www.football-data.co.uk/mmz4281/2425/SP1.csv", # La Liga
    "https://www.football-data.co.uk/mmz4281/2425/F1.csv",  # Ligue 1
    # "https://www.football-data.co.uk/mmz4281/2425/CL.csv",  # UCL (uncomment when published)
]

# 23/24 season (2324) — optional, gives more training data:
URLS_2324 = [
    "https://www.football-data.co.uk/mmz4281/2324/E0.csv",
    "https://www.football-data.co.uk/mmz4281/2324/D1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/I1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/SP1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/F1.csv",
    # "https://www.football-data.co.uk/mmz4281/2324/CL.csv",  # add if present
]

URLS = URLS_2425 + URLS_2324  # combine; comment out 2324 if you only want current season

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns.str.upper()
    df.columns = cols
    oddsH = next((c for c in ["B365H","PSH","WHH","IWH"] if c in df.columns), None)
    oddsD = next((c for c in ["B365D","PSD","WHD","IWD"] if c in df.columns), None)
    oddsA = next((c for c in ["B365A","PSA","WHA","IWA"] if c in df.columns), None)
    keep = {
        "date": "DATE","home_team":"HOMETEAM","away_team":"AWAYTEAM",
        "home_goals":"FTHG","away_goals":"FTAG",
        "home_odds_dec": oddsH,"draw_odds_dec": oddsD,"away_odds_dec": oddsA
    }
    out = {}
    for k,v in keep.items():
        out[k] = df[v] if v is not None and v in df.columns else pd.Series([None]*len(df))
    out_df = pd.DataFrame(out).dropna(subset=["date","home_team","away_team"])
    out_df["date"] = pd.to_datetime(out_df["date"], dayfirst=True, errors="coerce")
    out_df = out_df.dropna(subset=["date"])
    # sensible defaults; enrichment will override later
    for c in ["home_rest_days","away_rest_days"]: out_df[c]=4
    for c in ["home_travel_km","away_travel_km"]: out_df[c]=200
    for c in ["home_injury_index","away_injury_index"]: out_df[c]=0.3
    for c in ["home_gk_rating","away_gk_rating","home_setpiece_rating","away_setpiece_rating"]: out_df[c]=0.6
    out_df["ref_pen_rate"]=0.30; out_df["crowd_index"]=0.7
    return out_df

def main():
    frames=[]
    for u in URLS:
        try:
            df = download_csv(u)
            if df is not None and len(df):
                frames.append(normalize(df))
                print("OK:", u)
            else:
                print("Empty or invalid:", u)
        except Exception as e:
            print("Skipped:", u, "|", e)

    if frames:
        hist = pd.concat(frames, ignore_index=True).sort_values("date")
        hist.to_csv(OUT_HIST, index=False)
        print("Saved", OUT_HIST, len(hist))
    else:
        # Write an empty but correctly structured file so the pipeline can continue gracefully
        empty_cols = ["date","home_team","away_team","home_goals","away_goals",
                      "home_odds_dec","draw_odds_dec","away_odds_dec",
                      "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
                      "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
                      "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
        pd.DataFrame(columns=empty_cols).to_csv(OUT_HIST, index=False)
        print("Warning: no historical files fetched. Wrote empty schema to", OUT_HIST)

if __name__=="__main__":
    main()