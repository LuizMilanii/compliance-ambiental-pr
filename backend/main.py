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

def extrair_ano_prodes(props):
    """Extrai o ano da supressão florestal inspecionando todos os campos possíveis do INPE."""
    for campo in ["year", "ano", "def_year", "ano_prodes", "nu_ano", "anoref"]:
        if campo in props and props[campo] is not None:
            try:
                return int(props[campo])
            except (ValueError, TypeError):
                pass
    
    # Busca por padrões de 4 dígitos em campos textuais (ex: 'd2018', 'desmate_2016')
    for val in props.values():
        if isinstance(val, (str, int)):
            match = re.search(r"\b(20\d{2})\b", str(val))
            if match:
                return int(match.group(1))
    return None

@app.get("/")
def health_check():
    return {"status": "operacional", "estado": "Paraná (PR)"}

@app.get("/consultar/{cod_car}")
async def consultar_car(cod_car: str):
    cod_car = cod_car.strip().upper()

    # 1. Consulta no GeoParquet com conversão explícita ST_GeomFromWKB
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
    raw_geom = shape(json.loads(geojson_str))
    # Repara auto-interseções e topologias inválidas do polígono do CAR
    car_geom = raw_geom.buffer(0)
    minx, miny, maxx, maxy = car_geom.bounds

    # 2. Endpoints WFS oficiais
    wfs_ibama_url = (
        "https://geoserver.ibama.gov.br/geoserver/wfs?"
        "service=WFS&version=1.0.0&request=GetFeature&typeName=ibama:embargos&"
        f"outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy}"
    )

    # Variações de rotas do GeoServer do TerraBrasilis / INPE
    candidatos_prodes = [
        ("Mata Atlântica (OWS Global)", f"https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy},EPSG:4326"),
        ("Mata Atlântica (Workspace Virtual)", f"https://terrabrasilis.dpi.inpe.br/geoserver/prodes_mata_atlantica/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy},EPSG:4326"),
        ("Mata Atlântica (Fallback Layer)", f"https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica:prodes_mata_atlantica&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy},EPSG:4326"),
        ("Cerrado (OWS)", f"https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_cerrado:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={minx},{miny},{maxx},{maxy},EPSG:4326")
    ]

    ibama_intersect_area = 0.0
    ibama_features = []
    prodes_intersect_area = 0.0
    prodes_features = []
    anos_detectados = set()
    biomas_detectados = set()
    diagnostico = []

    # Desativação de verificação SSL (verify=False) para evitar recusa nos servidores federais
    async with httpx.AsyncClient(verify=False, timeout=25.0, follow_redirects=True) as client:
        # Consulta IBAMA
        try:
            resp_ibama = await client.get(wfs_ibama_url)
            if resp_ibama.status_code == 200:
                data_ib = resp_ibama.json()
                for feat in data_ib.get("features", []):
                    geom_e = shape(feat["geometry"]).buffer(0)
                    if car_geom.intersects(geom_e):
                        inter = car_geom.intersection(geom_e)
                        a_ha = calcular_area_ha(inter)
                        if a_ha > 0.0001:
                            ibama_intersect_area += a_ha
                            ibama_features.append(mapping(inter))
        except Exception as e:
            diagnostico.append(f"IBAMA falhou: {str(e)}")

        # Consulta PRODES
        for rotulo, url in candidatos_prodes:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                    except Exception:
                        diagnostico.append(f"{rotulo}: HTTP 200 recebido, mas resposta não é JSON (possível erro XML do GeoServer).")
                        continue

                    feats = data.get("features", [])
                    diagnostico.append(f"{rotulo}: {len(feats)} registros brutos encontrados na BBOX.")

                    if len(feats) > 0:
                        for feat in feats:
                            props = feat.get("properties", {})
                            ano = extrair_ano_prodes(props)

                            # Filtro temporal: somente eventos >= 2004
                            if ano is not None and ano < 2004:
                                continue

                            geom_p = shape(feat["geometry"]).buffer(0)
                            if car_geom.intersects(geom_p):
                                inter_p = car_geom.intersection(geom_p)
                                a_p = calcular_area_ha(inter_p)
                                if a_p > 0.0001:
                                    prodes_intersect_area += a_p
                                    biomas_detectados.add("Mata Atlântica" if "Mata" in rotulo else "Cerrado")
                                    if ano:
                                        anos_detectados.add(ano)
                                    prodes_features.append(mapping(inter_p))
                        
                        # Se já encontrou polígonos válidos na Mata Atlântica, não precisa rodar os fallbacks do mesmo bioma
                        if prodes_intersect_area > 0 and "Mata" in rotulo:
                            break
                else:
                    diagnostico.append(f"{rotulo}: Erro HTTP {resp.status_code}")
            except Exception as e:
                diagnostico.append(f"{rotulo}: Falha de conexão ({str(e)})")

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
            "diagnostico_servidores": diagnostico,
            "geojson_car": json.loads(geojson_str)
        }
    }
