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

app = FastAPI(title="Compliance Socioambiental PR - MCR 2-9")

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

# Endereços dos arquivos GeoParquet no GitHub Releases
URL_CAR_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/car_pr.parquet"
URL_PRODES_PARQUET = "https://github.com/LuizMilanii/compliance-ambiental-pr/releases/download/v1.0/prodes_pr.parquet"

# Projeção métrica para o Paraná (SIRGAS 2000 / UTM zone 22S - EPSG:31982)
proj_wgs84 = pyproj.CRS("EPSG:4326")
proj_utm_pr = pyproj.CRS("EPSG:31982")
transformer = pyproj.Transformer.from_crs(proj_wgs84, proj_utm_pr, always_xy=True).transform

def calcular_area_ha(geom):
    """Calcula a área precisa em hectares convertendo para projeção métrica UTM."""
    try:
        geom_utm = transform(transformer, geom)
        return round(geom_utm.area / 10000.0, 4)
    except Exception:
        return 0.0

# Tabela INCRA de Módulos Fiscais (ha) para municípios do Paraná
TABELA_MF_PR = {
    "CURITIBA": 10.0, "ITAPERUCU": 12.0, "ALMIRANTE TAMANDARE": 10.0,
    "RIO BRANCO DO SUL": 12.0, "COLOMBO": 10.0, "SAO JOSE DOS PINHAIS": 12.0,
    "PONTA GROSSA": 18.0, "CASTRO": 20.0, "IRATI": 18.0, "ARAPOTI": 18.0,
    "JAGUARIAIVA": 20.0, "GUARAPUAVA": 24.0, "PRUDENTOPOLIS": 18.0,
    "LONDRINA": 18.0, "MARINGA": 18.0, "CASCAVEL": 18.0, "TOLEDO": 18.0,
    "FOZ DO IGUACU": 16.0, "UMUARAMA": 20.0, "PARANAVAI": 20.0,
    "CAMPO MOURAO": 18.0, "PATO BRANCO": 18.0, "FRANCISCO BELTRAO": 18.0
}
MF_PADRAO_PR = 18.0

def obter_modulo_fiscal(nome_municipio: str) -> float:
    if not nome_municipio:
        return MF_PADRAO_PR
    mun_norm = re.sub(r"[^A-Z]", "", nome_municipio.upper())
    for k, v in TABELA_MF_PR.items():
        if k in mun_norm or mun_norm in k:
            return v
    return MF_PADRAO_PR

@app.get("/")
def health_check():
    return {"status": "operacional", "norma": "MCR 2-9 / Resolução CMN 5.303"}

@app.get("/consultar/{cod_car:path}")
async def consultar_car(cod_car: str):
    # 1. Normalização do CAR: limpa hífens, pontos e espaços
    cod_limpo = re.sub(r"[^A-Z0-9]", "", cod_car.upper())
    if not cod_limpo:
        raise HTTPException(status_code=400, detail="Código de CAR inválido.")

    # 2. Busca do Imóvel no Parquet do CAR
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
        raise HTTPException(status_code=500, detail=f"Erro ao consultar base do CAR: {str(e)}")

    if not res:
        raise HTTPException(status_code=404, detail="CAR não localizado na base do Paraná.")

    cod_imovel, municipio, uf, area_ha, geojson_str = res
    raw_geom = shape(json.loads(geojson_str))
    car_geom = raw_geom.buffer(0)
    minx, miny, maxx, maxy = car_geom.bounds

    # 3. Enquadramento Normativo MCR 2-9, Item 17 (Res. CMN 5.303)
    mf_ha = obter_modulo_fiscal(municipio)
    qtd_mf = round(area_ha / mf_ha, 2)

    if qtd_mf > 15.0:
        porte = "Superior a 15 módulos fiscais (Grande Propriedade)"
        alinea = "Alínea 'a'"
        vigencia = "04 de janeiro de 2027"
    elif qtd_mf > 4.0:
        porte = "Superior a 4 e de até 15 módulos fiscais (Média Propriedade)"
        alinea = "Alínea 'b'"
        vigencia = "1º de julho de 2027"
    else:
        porte = "De até 4 módulos fiscais (Pequena Propriedade / Minifúndio)"
        alinea = "Alínea 'c'"
        vigencia = "03 de janeiro de 2028"

    # 4. Interseção Espacial no PRODES Parquet
    query_prodes = f"""
        SELECT 
            ano,
            ST_AsGeoJSON(ST_GeomFromWKB(geom)) as geojson_desmat
        FROM read_parquet('{URL_PRODES_PARQUET}')
        WHERE ST_Intersects(ST_GeomFromWKB(geom), ST_GeomFromText('{car_geom.wkt}'));
    """

    prodes_features_mapa = []
    prodes_pos_2019_geoms = []
    prodes_pre_2019_geoms = []
    anos_pos_2019 = set()
    anos_pre_2019 = set()

    try:
        linhas_prodes = con.execute(query_prodes).fetchall()
        for ano_val, geojson_p in linhas_prodes:
            geom_p = shape(json.loads(geojson_p)).buffer(0)
            if car_geom.intersects(geom_p):
                inter = car_geom.intersection(geom_p)
                if not inter.is_empty:
                    if inter.geom_type == "GeometryCollection":
                        polys = [g for g in inter.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
                        inter = unary_union(polys) if polys else None

                    if inter and not inter.is_empty:
                        ano_int = int(ano_val) if ano_val is not None else None
                        area_fracao_ha = calcular_area_ha(inter)
                        # Marco MCR 17: supressão após 31/07/2019 (anos >= 2020)
                        eh_pos_2019 = bool(ano_int and ano_int >= 2020)

                        if eh_pos_2019:
                            prodes_pos_2019_geoms.append(inter)
                            anos_pos_2019.add(ano_int)
                        else:
                            prodes_pre_2019_geoms.append(inter)
                            if ano_int:
                                anos_pre_2019.add(ano_int)

                        prodes_features_mapa.append({
                            "type": "Feature",
                            "geometry": mapping(inter),
                            "properties": {
                                "ano": ano_int if ano_int else "N/D",
                                "area_ha": area_fracao_ha,
                                "pos_2019": eh_pos_2019
                            }
                        })
    except Exception as e:
        print(f"Aviso PRODES: {str(e)}")

    # 5. Consulta aos Embargos do IBAMA via WFS
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

    # 6. Consolidação e Deduplicação Espacial
    def unificar_geometrias(geoms):
        if not geoms:
            return 0.0, []
        uniao = unary_union(geoms)
        area = calcular_area_ha(uniao)
        geojsons = [mapping(uniao)] if uniao.geom_type in ["Polygon", "MultiPolygon"] else []
        return area, geojsons

    area_ibama, geo_ibama = unificar_geometrias(ibama_intersect_geoms)
    area_pos_2019, _ = unificar_geometrias(prodes_pos_2019_geoms)
    area_pre_2019, _ = unificar_geometrias(prodes_pre_2019_geoms)
    area_prodes_total, _ = unificar_geometrias(prodes_pos_2019_geoms + prodes_pre_2019_geoms)

    # 7. Redação do Parecer Técnico e Fundamentação
    tem_embargo = area_ibama > 0.0
    tem_desmat_pos_2019 = area_pos_2019 > 0.0

    if tem_embargo:
        status_geral = "IMPEDIMENTO IMEDIATO (MCR 2-9, ITEM 2)"
        descricao_impedimento = (
            f"O imóvel possui sobreposição de {area_ibama} ha com Embargo Ambiental Federal vigente do IBAMA. "
            "Pelo item 2 da Seção MCR 2-9 e Lei Federal nº 9.605/1998, é vedada a concessão de crédito rural para a área embargada."
        )
    elif tem_desmat_pos_2019:
        status_geral = f"IMPEDIMENTO VINCULADO AO MCR 2-9, ITEM 17 ({alinea.upper()})"[cite: 8]
        descricao_impedimento = (
            f"Detectada supressão de vegetação nativa após 31/07/2019, totalizando {area_pos_2019} ha (anos: {sorted(list(anos_pos_2019))}). "[cite: 8]
            f"Com dimensão de {area_ha} ha ({qtd_mf} Módulos Fiscais para {municipio}), o imóvel rural classifica-se como "
            f"'{porte}'. Nos termos da Resolução CMN nº 5.303 (art. 1º, MCR 2-9, item 17, {alinea}), "[cite: 8]
            f"o impedimento de crédito rural por supressão de vegetação nativa passa a ser mandatório a partir de {vigencia}."[cite: 8]
        )
    else:
        status_geral = "IMÓVEL CONFORME (MCR 2-9)"
        if area_pre_2019 > 0.0:
            descricao_impedimento = (
                f"Imóvel em conformidade para contratação de crédito rural. O desmatamento mapeado ({area_pre_2019} ha, anos: {sorted(list(anos_pre_2019))}) "
                "ocorreu antes do marco legal de 31/07/2019, não incidindo o bloqueio do item 17 do MCR 2-9 (Resolução CMN nº 5.303)."[cite: 8]
            )
        else:
            descricao_impedimento = (
                "Nenhum embargo ambiental do IBAMA ou polígono de desmatamento pelo PRODES (INPE) "
                "foi identificado sobre o perímetro cadastrado do imóvel rural."
            )

    return {
        "status": "sucesso",
        "dados": {
            "cod_imovel": cod_imovel,
            "municipio": municipio,
            "uf": uf,
            "area_declarada_ha": area_ha,
            "enquadramento_mcr_17": {
                "modulo_fiscal_municipio_ha": mf_ha,
                "qtd_modulos_fiscais": qtd_mf,
                "classificacao_porte": porte,
                "alinea_resolucao": alinea,
                "data_inicio_exigencia": vigencia,
                "marco_temporal": "Após 31 de julho de 2019",[cite: 8]
                "supressao_pos_2019_detectada": tem_desmat_pos_2019,
                "area_supressao_pos_2019_ha": area_pos_2019,
                "anos_pos_2019": sorted(list(anos_pos_2019)),
                "area_supressao_historica_pre_2019_ha": area_pre_2019,
                "anos_pre_2019": sorted(list(anos_pre_2019))
            },
            "parecer_normativo": {
                "bloqueio_credito": tem_embargo or tem_desmat_pos_2019,
                "status_conclusivo": status_geral,
                "descricao_detalhada": descricao_impedimento
            },
            "embargo_ibama": {
                "bloqueado": tem_embargo,
                "area_sobreposta_ha": area_ibama,
                "geometrias": geo_ibama
            },
            "desmatamento_prodes": {
                "detectado": area_prodes_total > 0.0,
                "area_total_ha": area_prodes_total,
                "features": prodes_features_mapa
            },
            "geojson_car": json.loads(geojson_str)
        }
    }
