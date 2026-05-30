"""Stadium → UF (Brazilian state) enrichment for the Brasileirao Silver layer.

Two lookup dicts shipped inline (zero external deps):

* ``STADIUM_TO_STATE``: stadium name (as returned by GE under ``sede.nome_popular``)
  → 2-letter UF code. Covers Série A 2026 + Série B 2026 + recent participants
  plus common stadium aliases (e.g. "Maracanã" and "Estádio Mário Filho").
* ``HOME_TEAM_TO_STATE``: team name (as returned by GE under
  ``equipes.mandante.nome_popular``) → 2-letter UF code. Used as a fallback
  when ``stadium`` is not in the first dict (estádio neutro, jogo emergencial,
  estádio novo ainda não catalogado).

The enrichment cascade is:
    1. STADIUM_TO_STATE lookup     → stadium_state, origin = 'STADIUM_LOOKUP'
    2. HOME_TEAM_TO_STATE lookup   → stadium_state, origin = 'HOME_TEAM_FALLBACK'
    3. Neither                     → stadium_state = 'UNKNOWN', origin = 'UNKNOWN'

The DataFrames returned by ``build_lookup_frames`` are tiny (~80 + ~50 rows)
so the Silver transform always wraps them in ``F.broadcast()`` — zero shuffle,
zero Python UDF serialization, everything stays in the JVM.

Migration path: when the dicts grow past hand-curation (>~200 entries), swap
the implementation here to ``spark.table('nessie.silver.brasileirao_stadium_lookup')``
and the rest of the Silver pipeline is unchanged.
"""

from pyspark.sql import DataFrame, SparkSession

# ──────────────────────────────────────────────────────────────────────────
# Seed: stadium → UF
# Keys mirror exactly what GE returns under ``sede.nome_popular``. Common
# aliases (oficial vs popular) are duplicated so both keys map to the same
# UF. Add a row when "Estádio desconhecido" warnings show up in production.
# ──────────────────────────────────────────────────────────────────────────
STADIUM_TO_STATE: dict[str, str] = {
    # ── Rio de Janeiro ──
    "Maracanã": "RJ",
    "Estádio Mário Filho": "RJ",
    "Nilton Santos": "RJ",
    "Engenhão": "RJ",
    "São Januário": "RJ",
    "Raulino de Oliveira": "RJ",
    "Giulite Coutinho": "RJ",
    "Luso-Brasileiro": "RJ",
    # ── São Paulo ──
    "Allianz Parque": "SP",
    "Morumbi": "SP",
    "MorumBis": "SP",
    "Cícero Pompeu de Toledo": "SP",
    "Neo Química Arena": "SP",
    "Itaquerão": "SP",
    "Arena Corinthians": "SP",
    "Vila Belmiro": "SP",
    "Urbano Caldeira": "SP",
    "Nabi Abi Chedid": "SP",
    "Arena Barueri": "SP",
    "Maião": "SP",
    "José Maria de Campos Maia": "SP",
    "Canindé": "SP",
    "1º de Maio": "SP",
    "Bruno José Daniel": "SP",
    "Moisés Lucarelli": "SP",
    "Brinco de Ouro da Princesa": "SP",
    "Jorge Ismael de Biasi": "SP",
    "Novelli Júnior": "SP",
    # ── Minas Gerais ──
    "Mineirão": "MG",
    "Magalhães Pinto": "MG",
    "Arena MRV": "MG",
    "Independência": "MG",
    "Raimundo Sampaio": "MG",
    "Almeidão": "PB",  # disambiguation note: Almeidão is in PB
    # ── Rio Grande do Sul ──
    "Arena do Grêmio": "RS",
    "Beira-Rio": "RS",
    "José Pinheiro Borda": "RS",
    "Alfredo Jaconi": "RS",
    "Centenário": "RS",
    # ── Paraná ──
    "Couto Pereira": "PR",
    "Major Antônio Couto Pereira": "PR",
    "Arena da Baixada": "PR",
    "Ligga Arena": "PR",
    "Joaquim Américo Guimarães": "PR",
    "Germano Krüger": "PR",
    "Vila Capanema": "PR",
    "Vitorino Gonçalves Dias": "PR",
    # ── Santa Catarina ──
    "Ressacada": "SC",
    "Aderbal Ramos da Silva": "SC",
    "Arena Condá": "SC",
    "Orlando Scarpelli": "SC",
    "Heriberto Hülse": "SC",
    "Augusto Bauer": "SC",
    # ── Bahia ──
    "Arena Fonte Nova": "BA",
    "Itaipava Fonte Nova": "BA",
    "Casa de Apostas Arena Fonte Nova": "BA",
    "Barradão": "BA",
    "Manoel Barradas": "BA",
    "Pituaçu": "BA",
    "Roberto Santos": "BA",
    # ── Pernambuco ──
    "Arena Pernambuco": "PE",
    "Cosme Damião": "PE",
    "Ilha do Retiro": "PE",
    "Adelmar da Costa Carvalho": "PE",
    "Aflitos": "PE",
    "Eládio de Barros Carvalho": "PE",
    "Arruda": "PE",
    "José do Rego Maciel": "PE",
    # ── Ceará ──
    "Castelão": "CE",  # NOTE: also a Castelão in MA — distinguishing on name alone is brittle
    "Plácido Aderaldo Castelo": "CE",
    "Arena Castelão": "CE",
    "Presidente Vargas": "CE",
    # ── Goiás ──
    "Serra Dourada": "GO",
    "Antônio Accioly": "GO",
    "Hailé Pinheiro": "GO",
    "OBA": "GO",
    # ── Mato Grosso ──
    "Arena Pantanal": "MT",
    "Eurico Gaspar Dutra": "MT",
    "Dutrinha": "MT",
    # ── Distrito Federal ──
    "Mané Garrincha": "DF",
    "Estádio Nacional": "DF",
    "Bezerrão": "DF",
    # ── Pará ──
    "Mangueirão": "PA",
    "Edgar Augusto Proença": "PA",
    "Curuzu": "PA",
    "Leônidas Sodré de Castro": "PA",
    "Baenão": "PA",
    "Evandro Almeida": "PA",
    # ── Amazonas ──
    "Arena da Amazônia": "AM",
    "Vivaldo Lima": "AM",
    "Carlos Zamith": "AM",
    "Ismael Benigno": "AM",
    "Colina": "AM",
    # ── Alagoas ──
    "Rei Pelé": "AL",
    "Estádio Rei Pelé": "AL",
    "Trapichão": "AL",
    "Gigante do Mutange": "AL",
    # ── Sergipe ──
    "Batistão": "SE",
    "Lourival Baptista": "SE",
    # ── Rio Grande do Norte ──
    "Arena das Dunas": "RN",
    "Frasqueirão": "RN",
    # ── Paraíba ──
    "Estádio Almeidão": "PB",
    "José Américo de Almeida Filho": "PB",
    # ── Espírito Santo ──
    "Kléber Andrade": "ES",
    "Estádio Engenheiro Araripe": "ES",
}


# ──────────────────────────────────────────────────────────────────────────
# Seed: home team → UF (fallback when stadium is unknown — neutral venue,
# new stadium, alias not yet mapped).
# Covers Série A 2026 + Série B 2026 + clubes que ciclaram entre A/B nos
# últimos 5 anos (cobertura defensiva pra rebaixamento/acesso anual).
# Keys mirror ``equipes.mandante.nome_popular`` from GE.
# ──────────────────────────────────────────────────────────────────────────
HOME_TEAM_TO_STATE: dict[str, str] = {
    # ── Rio de Janeiro ──
    "Flamengo": "RJ",
    "Fluminense": "RJ",
    "Botafogo": "RJ",
    "Vasco": "RJ",
    "Volta Redonda": "RJ",
    "Bangu": "RJ",
    "Madureira": "RJ",
    "Boavista": "RJ",
    "Portuguesa-RJ": "RJ",
    # ── São Paulo ──
    "Palmeiras": "SP",
    "São Paulo": "SP",
    "Corinthians": "SP",
    "Santos": "SP",
    "Bragantino": "SP",
    "Red Bull Bragantino": "SP",
    "Mirassol": "SP",
    "Botafogo-SP": "SP",
    "Guarani": "SP",
    "Ponte Preta": "SP",
    "Novorizontino": "SP",
    "Ituano": "SP",
    "São Bernardo": "SP",
    "Santo André": "SP",
    "Portuguesa": "SP",
    "Água Santa": "SP",
    "Inter de Limeira": "SP",
    # ── Minas Gerais ──
    "Atlético-MG": "MG",
    "Atlético": "MG",
    "Cruzeiro": "MG",
    "América-MG": "MG",
    "Tombense": "MG",
    "Uberlândia": "MG",
    "Athletic Club": "MG",
    # ── Rio Grande do Sul ──
    "Internacional": "RS",
    "Grêmio": "RS",
    "Juventude": "RS",
    "Caxias": "RS",
    "Brasil de Pelotas": "RS",
    "São José-RS": "RS",
    "Ypiranga-RS": "RS",
    # ── Paraná ──
    "Athletico-PR": "PR",
    "Athletico": "PR",
    "Coritiba": "PR",
    "Operário-PR": "PR",
    "Operário": "PR",
    "Londrina": "PR",
    "Paraná": "PR",
    "Cascavel": "PR",
    "Maringá": "PR",
    # ── Santa Catarina ──
    "Avaí": "SC",
    "Chapecoense": "SC",
    "Figueirense": "SC",
    "Criciúma": "SC",
    "Brusque": "SC",
    "Joinville": "SC",
    "Marcílio Dias": "SC",
    # ── Bahia ──
    "Bahia": "BA",
    "Vitória": "BA",
    "Atlético-BA": "BA",
    "Jacuipense": "BA",
    "Juazeirense": "BA",
    # ── Pernambuco ──
    "Sport": "PE",
    "Náutico": "PE",
    "Santa Cruz": "PE",
    "Retrô": "PE",
    "Salgueiro": "PE",
    # ── Ceará ──
    "Ceará": "CE",
    "Fortaleza": "CE",
    "Ferroviário": "CE",
    "Floresta": "CE",
    # ── Goiás ──
    "Goiás": "GO",
    "Atlético-GO": "GO",
    "Vila Nova": "GO",
    "Aparecidense": "GO",
    "Goianésia": "GO",
    # ── Mato Grosso ──
    "Cuiabá": "MT",
    "Mixto": "MT",
    "União Rondonópolis": "MT",
    # ── Mato Grosso do Sul ──
    "Operário-MS": "MS",
    "Costa Rica": "MS",
    # ── Distrito Federal ──
    "Brasiliense": "DF",
    "Gama": "DF",
    "Ceilândia": "DF",
    "Real Brasília": "DF",
    # ── Pará ──
    "Paysandu": "PA",
    "Remo": "PA",
    "Tuna Luso": "PA",
    "Águia de Marabá": "PA",
    # ── Amazonas ──
    "Amazonas": "AM",
    "Manaus": "AM",
    # ── Alagoas ──
    "CRB": "AL",
    "CSA": "AL",
    "ASA": "AL",
    # ── Sergipe ──
    "Sergipe": "SE",
    "Confiança": "SE",
    "Itabaiana": "SE",
    # ── Piauí ──
    "River-PI": "PI",
    "4 de Julho": "PI",
    "Altos": "PI",
    # ── Maranhão ──
    "Sampaio Corrêa": "MA",
    "Maranhão": "MA",
    "Moto Club": "MA",
    # ── Rio Grande do Norte ──
    "ABC": "RN",
    "América-RN": "RN",
    "Globo": "RN",
    # ── Paraíba ──
    "Botafogo-PB": "PB",
    "Treze": "PB",
    "Sousa": "PB",
    # ── Espírito Santo ──
    "Vitória-ES": "ES",
    "Desportiva Ferroviária": "ES",
    "Rio Branco-ES": "ES",
    # ── Tocantins ──
    "Tocantinópolis": "TO",
    "Palmas": "TO",
    # ── Acre ──
    "Atlético-AC": "AC",
    "Rio Branco-AC": "AC",
    # ── Roraima ──
    "São Raimundo-RR": "RR",
    # ── Rondônia ──
    "Genus": "RO",
    "União Cacoalense": "RO",
    # ── Amapá ──
    "Trem": "AP",
    "Ypiranga-AP": "AP",
}

# Constants for the origin column. Stable strings — Gold / dashboards may
# filter on them.
ORIGIN_STADIUM = "STADIUM_LOOKUP"
ORIGIN_HOME_TEAM = "HOME_TEAM_FALLBACK"
ORIGIN_UNKNOWN = "UNKNOWN"
SENTINEL_STATE = "UNKNOWN"


def build_lookup_frames(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Materialize the two lookup dicts as Spark DataFrames.

    Returned DataFrames are intended for ``F.broadcast()`` joins — tiny,
    static, no shuffle. Column names are prefixed to avoid clashing with
    the Bronze schema when joined.
    """
    stadiums = spark.createDataFrame(
        [(k, v) for k, v in STADIUM_TO_STATE.items()],
        ["_lookup_stadium", "_lookup_stadium_state"],
    )
    teams = spark.createDataFrame(
        [(k, v) for k, v in HOME_TEAM_TO_STATE.items()],
        ["_lookup_home_team", "_lookup_home_team_state"],
    )
    return stadiums, teams
