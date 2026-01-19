import pandas as pd
import json
import glob
import gzip

def extract_stats():
    stations_data = {}

    files = glob.glob("Q*_??_*_RR-T-Vent.csv.gz")

    for file in files:
        print(f"Traitement de {file}...")
        df = pd.read_csv(file, compression='gzip', sep=';', usecols=['NUM_POSTE', 'NOM_USUEL', 'LAT', 'LON', 'AAAAMMJJ', 'TN'])

        df['AAAAMMJJ'] = pd.to_datetime(df['AAAAMMJJ'], format='%Y%m%d')
        df['year'] = df['AAAAMMJJ'].dt.year
        df['month'] = df['AAAAMMJJ'].dt.month

        current_year = 2026

        for num_poste, group in df.groupby('NUM_POSTE'):
            station_info = {
                "name": group['NOM_USUEL'].iloc[0],
                "lat": float(group['LAT'].iloc[0]),
                "lon": float(group['LON'].iloc[0]),
                "stats": {}
            }

            for period in [5, 10, 20]:
                start_year = current_year - period
                d_period = group[group['year'] >= start_year]

                if not d_period.empty:
                    min_abs = d_period['TN'].min()

                    months_stats = {}
                    for m in [9, 10, 11, 12, 1, 2, 3, 4]:
                        m_min = d_period[d_period['month'] == m]['TN'].min()
                        months_stats[f"month_{m}"] = float(m_min) if pd.notnull(m_min) else None

                    station_info["stats"][f"last_{period}y"] = {
                        "min_absolute": float(min_abs) if pd.notnull(min_abs) else None,
                        "by_month": months_stats
                    }

            stations_data[num_poste] = station_info

    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for id, info in stations_data.items():
        if ("last_20y" in info["stats"] and info["stats"]["last_20y"]["min_absolute"] is not None) \
            or ("last_10y" in info["stats"] and info["stats"]["last_10y"]["min_absolute"] is not None) \
            or ("last_5y" in info["stats"] and info["stats"]["last_5y"]["min_absolute"] is not None):
            feature = {
                "type": "Feature",
                "properties": {
                    "id": id,
                    "nom": info["name"],
                    **info["stats"]
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [info["lon"], info["lat"]]
                }
            }
            geojson["features"].append(feature)

    with open("stations_temperatures.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)

extract_stats()
