import json
import gzip
import geopandas as gpd
from pathlib import Path
from config_app import GEOJSON_PATH

print("Chargement du GeoJSON...")
with open(GEOJSON_PATH) as f:
    geojson = json.load(f)

print("Simplification des géométries...")
gdf = gpd.GeoDataFrame.from_features(geojson["features"])
gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.0005)
simplified = json.loads(gdf.to_json())

print("Compression...")
output_path = Path(str(GEOJSON_PATH) + ".gz")
with gzip.open(output_path, 'wt', encoding='utf-8') as f:
    json.dump(simplified, f)

original_size = GEOJSON_PATH.stat().st_size / (1024*1024)
compressed_size = output_path.stat().st_size / (1024*1024)

print(f"✓ Original : {original_size:.1f} MB")
print(f"✓ Compressé : {compressed_size:.1f} MB")
print(f"✓ Ratio : {compressed_size/original_size:.1%}")