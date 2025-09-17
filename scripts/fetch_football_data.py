import os, pandas as pd
from utils import download_csv

OUT_HIST = "data/raw_football_data.csv"
os.makedirs("data", exist_ok=True)

URLS = [
    "https://www.football-data.co.uk/mmz4281/2425/CL.csv",  # UCL 24/25
    # Add more league CSVs if desired (EPL, La Liga, etc.)
]

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
        out[k] = df[v] if v in df.columns else pd.Series([None]*len(df))
    out_df = pd.DataFrame(out).dropna(subset=["date","home_team","away_team"])
    out_df["date"] = pd.to_datetime(out_df["date"], dayfirst=True, errors="coerce")
    out_df = out_df.dropna(subset=["date"])
    # default values
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
            df=download_csv(u); frames.append(normalize(df))
        except Exception as e: print("Failed:",u,e)
    hist=pd.concat(frames,ignore_index=True).sort_values("date")
    hist.to_csv(OUT_HIST,index=False)
    print("Saved",OUT_HIST,len(hist))

if __name__=="__main__": main()
