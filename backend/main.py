import json
import duckdb
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from shapely.geometry import shape, mapping
from shapely.ops import transform
import pyproj

app = FastAPI(title="Compliance Socioambiental PR")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão DuckDB em memória com extensões espaciais e leitura remota HTTP
con = duckdb.connect(database=':memory:')
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

# Ligação direta à base do CAR-PR publicada na release do GitHub
URL_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/car_pr.parquet"

# Projeção SIRGAS 2000 UTM 22S para cálculo métrico exato de hectares no Paraná
proj_wgs84 = pyproj.CRS("EPSG:4326")
proj_utm_pr = pyproj.CRS("EPSG:31982")
transformer = pyproj.Transformer.from_crs(proj_wgs84, proj_utm_pr, always_xy=True).transform

def calcular_area_ha(geom):
    geom_utm = transform(transformer, geom)
    return round(geom_utm.area / 10000, 4)

@app.get("/")
def health_check():
    return {"status": "operacional", "estado": "Paraná (PR)"}

@app.get("/consultar/{cod_car}")
async def consultar_car(cod_car: str):
    cod_car = cod_car.strip().upper()
    
# Consulta seletiva com conversão explícita do binário WKB para geometria
    query = f"""
        SELECT cod_imovel, municipio, uf, area_ha, ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson
        FROM read_parquet('{URL_PARQUET}')
        WHERE UPPER(cod_imovel) = ?
        LIMIT 1;
    """
    try:
        res = con.execute(query, [cod_car]).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar a base: {str(e)}")

    if not res:
        raise HTTPException(status_code=404, detail="CAR não localizado na base do Paraná.")
        
    cod_imovel, municipio, uf, area_ha, geojson_str = res
    car_geom = shape(json.loads(geojson_str))
    minx, miny, maxx, maxy = car_geom.bounds

    # Consulta WFS dos Embargos Federais do IBAMA
    wfs_ibama_url = (
        "https://geoserver.ibama.gov.br/geoserver/wfs?"
        "service=WFS&version=1.0.0&request=GetFeature&typeName=ibama:embargos&"
        f"outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy},EPSG:4326"
    )

    ibama_intersect_area = 0.0
    ibama_features = []
    
    async with httpx.AsyncClient(timeout=12.0) as client:
        try:
            resp = await client.get(wfs_ibama_url)
            if resp.status_code == 200:
                data = resp.json()
                for feat in data.get("features", []):
                    geom_embargo = shape(feat["geometry"])
                    if car_geom.intersects(geom_embargo):
                        inter = car_geom.intersection(geom_embargo)
                        area_inter = calcular_area_ha(inter)
                        if area_inter > 0.0001:
                            ibama_intersect_area += area_inter
                            ibama_features.append(mapping(inter))
        except Exception:
            pass

    return {
        "status": "sucesso",
        "dados": {
            "cod_imovel": cod_imovel,
            "municipio": municipio,
            "uf": uf,
            "area_declarada_ha": area_ha,
            "embargo_ibama": {
                "bloqueado": ibama_intersect_area > 0.0,
                "area_sobreposta_ha": ibama_intersect_area,
                "geometrias": ibama_features
            },
            "geojson_car": json.loads(geojson_str)
        }
    }
