import pandas as pd, os

HIST_IN="data/raw_football_data.csv"
UPCOMING_IN="data/raw_theodds_fixtures.csv"
HIST_OUT="data/HIST_matches.csv"
UPCOMING_OUT="data/UPCOMING_fixtures.csv"

def reorder_hist(df):
    cols=["date","home_team","away_team","home_goals","away_goals",
          "home_odds_dec","draw_odds_dec","away_odds_dec",
          "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    return df[cols]

def reorder_upc(df):
    cols=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec",
          "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    return df[cols]

def main():
    hist=pd.read_csv(HIST_IN,parse_dates=["date"])
    upc=pd.read_csv(UPCOMING_IN,parse_dates=["date"])
    hist=reorder_hist(hist).sort_values("date")
    upc=reorder_upc(upc).sort_values("date")
    os.makedirs("data",exist_ok=True)
    hist.to_csv(HIST_OUT,index=False)
    upc.to_csv(UPCOMING_OUT,index=False)
    print("Built:",HIST_OUT,len(hist),"|",UPCOMING_OUT,len(upc))

if __name__=="__main__": main()
