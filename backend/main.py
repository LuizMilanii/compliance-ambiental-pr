import asyncio
import json
import re
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

# DuckDB em memória com extensões espaciais e httpfs
con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

URL_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/car_pr.parquet"

# Projeção métrica para o Paraná (SIRGAS 2000 / UTM 22S - EPSG:31982)
proj_wgs84 = pyproj.CRS("EPSG:4326")
proj_utm_pr = pyproj.CRS("EPSG:31982")
transformer = pyproj.Transformer.from_crs(proj_wgs84, proj_utm_pr, always_xy=True).transform

def calcular_area_ha(geom):
    """Calcula a área precisa em hectares convertendo para projeção UTM."""
    geom_utm = transform(transformer, geom)
    return round(geom_utm.area / 10000.0, 4)

def extrair_ano_prodes(props):
    """Extrai o ano da supressão florestal a partir dos diferentes padrões de campos do INPE."""
    for campo in ["year", "ano", "def_year", "ano_prodes"]:
        if campo in props and props[campo]:
            try:
                return int(props[campo])
            except (ValueError, TypeError):
                pass
    
    # Busca em campos textuais como class_name ou observation (ex: 'd2018', 'desmatamento_2020')
    for texto in [props.get("class_name"), props.get("image_date"), props.get("view_date")]:
        if texto:
            match = re.search(r"\b(20\d{2})\b", str(texto))
            if match:
                return int(match.group(1))
    return None

@app.get("/")
def health_check():
    return {"status": "operacional", "estado": "Paraná (PR)"}

@app.get("/consultar/{cod_car}")
async def consultar_car(cod_car: str):
    cod_car = cod_car.strip().upper()

    # 1. Consulta no GeoParquet com conversão ST_GeomFromWKB
    query = f"""
        SELECT cod_imovel, municipio, uf, area_ha, ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson
        FROM read_parquet('{URL_PARQUET}')
        WHERE UPPER(cod_imovel) = ?
        LIMIT 1;
    """
    try:
        res = con.execute(query, [cod_car]).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar base local: {str(e)}")

    if not res:
        raise HTTPException(status_code=404, detail="CAR não localizado na base do Paraná.")

    cod_imovel, municipio, uf, area_ha, geojson_str = res
    car_geom = shape(json.loads(geojson_str))
    minx, miny, maxx, maxy = car_geom.bounds

    # 2. URLs de WFS com formato BBOX correto para WFS 1.0.0
    wfs_ibama_url = (
        "https://geoserver.ibama.gov.br/geoserver/wfs?"
        "service=WFS&version=1.0.0&request=GetFeature&typeName=ibama:embargos&"
        f"outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}"
    )

    # Nomenclaturas reais do TerraBrasilis
    urls_prodes = [
        ("Mata Atlântica", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}"),
        ("Mata Atlântica (Fallback)", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica_yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}"),
        ("Cerrado", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_cerrado:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}")
    ]

    ibama_intersect_area = 0.0
    ibama_features = []
    prodes_intersect_area = 0.0
    prodes_features = []
    anos_detectados = set()
    biomas_detectados = set()
    debug_logs = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        # Consulta IBAMA
        try:
            resp_ibama = await client.get(wfs_ibama_url)
            if resp_ibama.status_code == 200:
                data_ib = resp_ibama.json()
                for feat in data_ib.get("features", []):
                    geom_e = shape(feat["geometry"])
                    if car_geom.intersects(geom_e):
                        inter = car_geom.intersection(geom_e)
                        a_ha = calcular_area_ha(inter)
                        if a_ha > 0.0001:
                            ibama_intersect_area += a_ha
                            ibama_features.append(mapping(inter))
        except Exception as e:
            debug_logs.append(f"IBAMA falhou: {str(e)}")

        # Consulta PRODES
        for bioma_label, url in urls_prodes:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                    except Exception:
                        debug_logs.append(f"{bioma_label}: retornou status 200 mas resposta não era JSON.")
                        continue

                    feats = data.get("features", [])
                    debug_logs.append(f"{bioma_label}: retornou {len(feats)} registros brutos no BBOX.")

                    for feat in feats:
                        props = feat.get("properties", {})
                        ano = extrair_ano_prodes(props)

                        # Filtro MCR: apenas eventos a partir de 2004
                        if ano is not None and ano < 2004:
                            continue

                        geom_p = shape(feat["geometry"])
                        if car_geom.intersects(geom_p):
                            inter_p = car_geom.intersection(geom_p)
                            a_p = calcular_area_ha(inter_p)
                            if a_p > 0.0001:
                                prodes_intersect_area += a_p
                                biomas_detectados.add(bioma_label.split()[0])
                                if ano:
                                    anos_detectados.add(ano)
                                prodes_features.append(mapping(inter_p))
                else:
                    debug_logs.append(f"{bioma_label}: HTTP {resp.status_code}")
            except Exception as e:
                debug_logs.append(f"{bioma_label} erro conexão: {str(e)}")

    bloqueado = (ibama_intersect_area > 0.0) or (prodes_intersect_area > 0.0)

    return {
        "status": "sucesso",
        "dados": {
            "cod_imovel": cod_imovel,
            "municipio": municipio,
            "uf": uf,
            "area_declarada_ha": area_ha,
            "bloqueio_credito": bloqueado,
            "embargo_ibama": {
                "bloqueado": ibama_intersect_area > 0.0,
                "area_sobreposta_ha": round(ibama_intersect_area, 4),
                "geometrias": ibama_features
            },
            "desmatamento_prodes": {
                "criterio_temporal": ">= 2004",
                "detectado": prodes_intersect_area > 0.0,
                "area_sobreposta_ha": round(prodes_intersect_area, 4),
                "anos_detectados": sorted(list(anos_detectados)),
                "biomas_detectados": sorted(list(biomas_detectados)),
                "geometrias": prodes_features
            },
            "diagnostico_servidores": debug_logs,
            "geojson_car": json.loads(geojson_str)
        }
    }
