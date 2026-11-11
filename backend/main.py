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

# Conexão DuckDB em memória com suporte espacial e leitura remota HTTP
con = duckdb.connect(database=":memory:")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

# Base GeoParquet do CAR-PR
URL_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/car_pr.parquet"

# Projeção métrica para o Paraná (SIRGAS 2000 / UTM zone 22S - EPSG:31982)
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
    """Extrai o ano da supressão florestal a partir dos atributos do INPE."""
    for campo in ["year", "ano", "def_year", "ano_prodes", "nu_ano", "anoref"]:
        if campo in props and props[campo] is not None:
            try:
                return int(props[campo])
            except (ValueError, TypeError):
                pass
    for val in props.values():
        if isinstance(val, (str, int)):
            match = re.search(r"\b(20\d{2}|19\d{2})\b", str(val))
            if match:
                return int(match.group(1))
    return None

@app.get("/")
def health_check():
    return {"status": "operacional", "estado": "Paraná (PR)"}

@app.get("/consultar/{cod_car:path}")
async def consultar_car(cod_car: str):
    # 1. Normalização do CAR: remove pontuações, traços, barras e espaços
    cod_limpo = re.sub(r"[^A-Z0-9]", "", cod_car.upper())
    if not cod_limpo:
        raise HTTPException(status_code=400, detail="Código de CAR inválido.")

    # 2. Busca flexível no GeoParquet (reconhece com máscara, sem máscara ou sem 'PR')
    query = f"""
        SELECT cod_imovel, municipio, uf, area_ha, ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson
        FROM read_parquet('{URL_PARQUET}')
        WHERE regexp_replace(UPPER(cod_imovel), '[^A-Z0-9]', '', 'g') = ?
           OR regexp_replace(UPPER(cod_imovel), '[^A-Z0-9]', '', 'g') = 'PR' || ?
        LIMIT 1;
    """
    try:
        res = con.execute(query, [cod_limpo, cod_limpo]).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar a base do CAR: {str(e)}")

    if not res:
        raise HTTPException(status_code=404, detail="CAR não localizado na base do Paraná.")

    cod_imovel, municipio, uf, area_ha, geojson_str = res
    raw_geom = shape(json.loads(geojson_str))
    car_geom = raw_geom.buffer(0)  # Corrige auto-interseções topológicas

    # Margem de segurança de ~300 metros na BBOX para não truncar feições de borda
    pad = 0.003
    minx, miny, maxx, maxy = car_geom.bounds
    q_minx, q_miny, q_maxx, q_maxy = minx - pad, miny - pad, maxx + pad, maxy + pad

    # 3. Definição das camadas e servidores de monitoramento
    wfs_ibama_url = (
        "https://geoserver.ibama.gov.br/geoserver/wfs?"
        "service=WFS&version=1.0.0&request=GetFeature&typeName=ibama:embargos&"
        f"outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"
    )

    # Varredura completa das bases do INPE / TerraBrasilis
    camadas_inpe = [
        ("PRODES Mata Atlântica", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"),
        ("PRODES Mata Atlântica (hífen)", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes-mata-atlantica:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"),
        ("PRODES Mata Atlântica (geral)", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_mata_atlantica:prodes_mata_atlantica&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"),
        ("PRODES Cerrado", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes_cerrado:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"),
        ("PRODES Cerrado (hífen)", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=prodes-cerrado:yearly_deforestation&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}"),
        ("DETER Mata Atlântica", f"https://terrabrasilis.dpi.inpe.br/geoserver/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=deter_mata_atlantica:deter_mata_atlantica&outputFormat=application/json&srsName=EPSG:4326&bbox={q_minx},{q_miny},{q_maxx},{q_maxy}")
    ]

    headers_web = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }

    ibama_intersect_geoms = []
    prodes_intersect_geoms = []
    anos_detectados = set()
    bases_detectadas = set()
    diagnostico = []

    async with httpx.AsyncClient(verify=False, timeout=25.0, headers=headers_web, follow_redirects=True) as client:
        # Consulta IBAMA
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
        except Exception as e:
            diagnostico.append(f"IBAMA falhou: {str(e)}")

        # Consultas concorrentes ao INPE
        tasks = [client.get(url) for _, url in camadas_inpe]
        respostas = await asyncio.gather(*tasks, return_exceptions=True)

        for (label, _), resp in zip(camadas_inpe, respostas):
            if isinstance(resp, httpx.Response) and resp.status_code == 200:
                try:
                    data_inpe = resp.json()
                except Exception:
                    continue

                feats = data_inpe.get("features", [])
                if feats:
                    diagnostico.append(f"{label}: {len(feats)} registros brutos na região")

                for feat in feats:
                    geom_raw = feat.get("geometry")
                    if not geom_raw:
                        continue

                    geom_p = shape(geom_raw).buffer(0)
                    if car_geom.intersects(geom_p):
                        inter = car_geom.intersection(geom_p)
                        if inter.is_empty:
                            continue

                        # Isola apenas polígonos caso retorne GeometryCollection
                        if inter.geom_type == "GeometryCollection":
                            polys = [g for g in inter.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
                            if polys:
                                inter = unary_union(polys)
                            else:
                                continue

                        prodes_intersect_geoms.append(inter)
                        bases_detectadas.add(label.split()[0])
                        ano = extrair_ano_prodes(feat.get("properties", {}))
                        if ano:
                            anos_detectados.add(ano)

    # 4. Cálculo de áreas consolidadas com união geométrica (sem duplicar hectares)
    ibama_area_ha = 0.0
    ibama_geojson = []
    if ibama_intersect_geoms:
        uniao_ibama = unary_union(ibama_intersect_geoms)
        ibama_area_ha = calcular_area_ha(uniao_ibama)
        if uniao_ibama.geom_type in ["Polygon", "MultiPolygon"]:
            ibama_geojson = [mapping(uniao_ibama)]

    prodes_area_ha = 0.0
    prodes_geojson = []
    if prodes_intersect_geoms:
        uniao_prodes = unary_union(prodes_intersect_geoms)
        prodes_area_ha = calcular_area_ha(uniao_prodes)
        if uniao_prodes.geom_type in ["Polygon", "MultiPolygon"]:
            prodes_geojson = [mapping(uniao_prodes)]

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
                "criterio_temporal": "Histórico Completo (Sem restrição de data)",
                "detectado": prodes_area_ha > 0.0,
                "area_sobreposta_ha": prodes_area_ha,
                "anos_detectados": sorted(list(anos_detectados)),
                "bases_detectadas": sorted(list(bases_detectadas)),
                "geometrias": prodes_geojson
            },
            "diagnostico_servidores": diagnostico,
            "geojson_car": json.loads(geojson_str)
        }
    }
