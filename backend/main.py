import asyncio
import json
import re
import duckdb
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from shapely.geometry import shape, mapping
from shapely.ops import transform, unary_union
import pyproj

app = FastAPI(title="Compliance Socioambiental PR")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão DuckDB em memória com extensões espaciais e httpfs
con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

URL_CAR_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/car_pr.parquet"
URL_PRODES_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/prodes_pr.parquet"

# Projeção UTM 22S para cálculo preciso de áreas no Paraná
proj_wgs84 = pyproj.CRS("EPSG:4326")
proj_utm_pr = pyproj.CRS("EPSG:31982")
transformer = pyproj.Transformer.from_crs(proj_wgs84, proj_utm_pr, always_xy=True).transform

def calcular_area_ha(geom):
    """Calcula a área precisa em hectares convertendo para projeção UTM."""
    try:
        geom_utm = transform(transformer, geom)
        return round(geom_utm.area / 10000.0, 4)
    except Exception:
        return 0.0

@app.get("/")
def health_check():
    return {"status": "operacional", "estado": "Paraná (PR)"}

@app.get("/consultar/{cod_car:path}")
async def consultar_car(cod_car: str):
    # 1. Normalização do CAR: remove hífens, pontos, barras e espaços
    cod_limpo = re.sub(r"[^A-Z0-9]", "", cod_car.upper())
    if not cod_limpo:
        raise HTTPException(status_code=400, detail="Código de CAR inválido.")

    # 2. Busca do CAR no Parquet (aceita com ou sem máscara e sem prefixo 'PR')
    query_car = f"""
        SELECT cod_imovel, municipio, uf, area_ha, ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson
        FROM read_parquet('{URL_CAR_PARQUET}')
        WHERE regexp_replace(UPPER(cod_imovel), '[^A-Z0-9]', '', 'g') = ?
           OR regexp_replace(UPPER(cod_imovel), '[^A-Z0-9]', '', 'g') = 'PR' || ?
        LIMIT 1;
    """
    try:
        res = con.execute(query_car, [cod_limpo, cod_limpo]).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar a base do CAR: {str(e)}")

    if not res:
        raise HTTPException(status_code=404, detail="CAR não localizado na base do Paraná.")

    cod_imovel, municipio, uf, area_ha, geojson_str = res
    raw_geom = shape(json.loads(geojson_str))
    car_geom = raw_geom.buffer(0)
    minx, miny, maxx, maxy = car_geom.bounds

    # 3. Consulta Espacial no PRODES Parquet local via DuckDB
    query_prodes = f"""
        SELECT 
            ano,
            ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson_desmat
        FROM read_parquet('{URL_PRODES_PARQUET}')
        WHERE ST_Intersects(ST_GeomFromWKB(geom), ST_GeomFromText('{car_geom.wkt}'));
    """
    
    prodes_intersect_geoms = []
    anos_detectados = set()

    try:
        linhas_prodes = con.execute(query_prodes).fetchall()
        for ano, geojson_p in linhas_prodes:
            geom_p = shape(json.loads(geojson_p)).buffer(0)
            if car_geom.intersects(geom_p):
                inter = car_geom.intersection(geom_p)
                if not inter.is_empty:
                    if inter.geom_type == "GeometryCollection":
                        polys = [g for g in inter.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
                        if polys:
                            inter = unary_union(polys)
                        else:
                            continue
                    prodes_intersect_geoms.append(inter)
                    if ano:
                        anos_detectados.add(int(ano))
    except Exception as e:
        print(f"Aviso PRODES: {str(e)}")

    # 4. Consulta IBAMA via WFS
    wfs_ibama_url = (
        "https://geoserver.ibama.gov.br/geoserver/wfs?"
        "service=WFS&version=1.0.0&request=GetFeature&typeName=ibama:embargos&"
        f"outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}"
    )

    ibama_intersect_geoms = []
    async with httpx.AsyncClient(verify=False, timeout=20.0, follow_redirects=True) as client:
        try:
            resp_ibama = await client.get(wfs_ibama_url)
            if resp_ibama.status_code == 200:
                data_ib = resp_ibama.json()
                for feat in data_ib.get("features", []):
                    geom_e = shape(feat["geometry"]).buffer(0)
                    if car_geom.intersects(geom_e):
                        inter = car_geom.intersection(geom_e)
                        if not inter.is_empty:
                            ibama_intersect_geoms.append(inter)
        except Exception:
            pass

    # 5. Consolidação e Deduplicação Espacial das Áreas
    prodes_area_ha = 0.0
    prodes_geojson = []
    if prodes_intersect_geoms:
        uniao_prodes = unary_union(prodes_intersect_geoms)
        prodes_area_ha = calcular_area_ha(uniao_prodes)
        if uniao_prodes.geom_type in ["Polygon", "MultiPolygon"]:
            prodes_geojson = [mapping(uniao_prodes)]

    ibama_area_ha = 0.0
    ibama_geojson = []
    if ibama_intersect_geoms:
        uniao_ibama = unary_union(ibama_intersect_geoms)
        ibama_area_ha = calcular_area_ha(uniao_ibama)
        if uniao_ibama.geom_type in ["Polygon", "MultiPolygon"]:
            ibama_geojson = [mapping(uniao_ibama)]

    bloqueado = (ibama_area_ha > 0.0) or (prodes_area_ha > 0.0)

    return {
        "status": "sucesso",
        "dados": {
            "cod_imovel": cod_imovel,
            "municipio": municipio,
            "uf": uf,
            "area_declarada_ha": area_ha,
            "bloqueio_credito": bloqueado,
            "embargo_ibama": {
                "bloqueado": ibama_area_ha > 0.0,
                "area_sobreposta_ha": ibama_area_ha,
                "geometrias": ibama_geojson
            },
            "desmatamento_prodes": {
                "fonte": "PRODES Mata Atlântica (INPE)",
                "detectado": prodes_area_ha > 0.0,
                "area_sobreposta_ha": prodes_area_ha,
                "anos_detectados": sorted(list(anos_detectados)),
                "geometrias": prodes_geojson
            },
            "geojson_car": json.loads(geojson_str)
        }
    }
