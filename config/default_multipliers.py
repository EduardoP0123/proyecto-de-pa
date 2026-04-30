"""
Multipliers por defecto para empresas ZFSI.
Fuente de verdad: MEDICIONES ZFSI MES DE MARZO 2026.xlsx

El sistema los aplica automáticamente al detectar una empresa,
usando matching normalizado para tolerar variaciones de nombre.
El usuario puede sobreescribir cualquier valor en la UI.
"""

import re

# ---------------------------------------------------------------------------
# Catálogo oficial (nombres del Excel + variantes conocidas de archivos CSV)
# ---------------------------------------------------------------------------
DEFAULT_MULTIPLIERS: dict[str, int] = {

    # 1 — ADM ACUEDUCTO / ADMIN ACUEDUCTO
    "adm acueducto":            80,
    "admin acueducto":          80,

    # 2 — ADM OFICINA / ADMIN OFICINA
    "adm oficina":              80,
    "admin oficina":            80,

    # 3 — ADMIN AREA COM
    "admin area com":           80,
    "adm area com":             80,
    "area comun arriol":        80,   # variante CSV
    "area comun arrior":        80,   # typo CSV
    "admin area comun arriol":  80,

    # 4 — POINT BLANK
    "point blank":              900,
    "pointblank":               900,

    # 5 — BANCO LEON
    "banco leon":               1,

    # 6 — MAHOLDING!A1  (el ! se elimina al normalizar)
    "maholding":                900,
    "maholding a1":             900,
    "edificio mador":           900,  # alias que aparece en algunos archivos

    # 7 — MFI 3
    "mfi 3":                    600,
    "mfi3":                     600,

    # 8 — MFI AUTO MULTIUSOS
    "mfi auto multiusos":       40,
    "mfi auto":                 40,

    # 9 — GILDAN ZFSI
    "gildan zfsi":              600,
    "gildan":                   600,

    # 10 — ALPHA PLASTICS
    "alpha plastics":           480,
    "alpha":                    480,

    # 11 — ALESSI DOMENICO
    "alessi domenico":          360,
    "alessi":                   360,

    # 12 — SCHAD 2
    "schad 2":                  300,
    "schad ii":                 300,
    "schad2":                   300,

    # 13 — E-CYCLING
    "e-cycling":                40,
    "e cycling":                40,
    "ecycling":                 40,
    "ecycling 1":               40,
    "e-cycling 1":              40,

    # 14-16, 19 — ILUMINACIONES (multiplo 1)
    "iluminacion 1":            1,
    "iluminacion 2":            1,
    "iluminacion 3":            1,
    "iluminacion 6":            1,
    "ilu-parq. ext. 1":         1,
    "ilu-parq. int. 1":         1,
    "ilu-policlinica":          1,
    "ilu-circ.1":               1,
    "ilu circ 1":               1,

    # 17 — POINT BLANK 2
    "point blank 2":            480,
    "pointblank 2":             480,
    "poink black 2":            480,  # typo CSV

    # 18 — HANES
    "hanes":                    1800,
    "hanes caribe":             1800,

    # 20 — NATIONAL CHAIN (NUEVO)
    "national chain nuevo":     120,
    "national chain (nuevo)":   120,
    "national nuevo":           120,  # nombre de hoja Excel

    # 21 — RITRAMA
    "ritrama":                  80,

    # 22 — JOHNSON
    "johnson":                  80,

    # 23 — TONKA
    "tonka":                    1200,
    "tomka":                    1200,  # typo CSV

    # 24 — E-CYCLING-2
    "e-cycling-2":              80,
    "e cycling 2":              80,
    "ecycling 2":               80,
    "e-cycling 2":              80,
    "merit caribbean":          80,   # alias que aparece en algunos archivos

    # 25 — MFI BT
    "mfi bt":                   200,

    # 26 — CORFLEX
    "corflex":                  80,

    # 27 — MONTESINO
    "montesino":                80,
    "national montesino":       80,
    "national mantesino":       80,   # typo CSV

    # 28 — GIVONA
    "givona":                   40,

    # 29 — SCHAD V
    "schad v":                  80,

    # 30 — TELEPUERTO
    "telepuerto":               80,

    # 31 — BANCO RESERVAS
    "banco reservas":           1,

    # 32 — INGENIUM DEUS
    "ingenium deus":            80,
    "ingenium deus 1":          80,
    "ingenium 1":               80,
    "ingenium1":                80,

    # 33 — INGENIUM DEUS 2
    "ingenium deus 2":          40,
    "ingenium 2":               40,
    "ingenium2":                40,

    # 34 — MFI 4
    "mfi 4":                    480,
    "mfi4":                     480,
    "mfi-4":                    480,
    "acs":                      480,  # alias en algunos archivos

    # 35 — SANDCASTLE
    "sandcastle":               600,

    # 36 — PRIME
    "prime":                    60,
    "prime technology":         60,

    # 37 — K&L MICROWAVE
    "k&l microwave":            240,
    "kl microwave":             240,
    "k l microwave":            240,
    "k&l microware":            240,  # typo CSV
    "kl microware":             240,  # typo CSV sin &

    # 38 — PUERTA III
    "puerta iii":               3,
    "puerta 3":                 3,
    "puerta lll":               3,    # L minúsculas como número

    # 39 — ALUMAXINC1
    "alumaxinc1":               80,
    "alumaxinc 1":              80,
    "alumaxin 1":               80,
    "alumaxin i":               80,
    "alumaxin l":               80,   # L como I

    # 40 — ALTEX CARIBE
    "altex caribe":             1,

    # 41 — SCHAD FREE
    "schad free":               240,

    # 42 — PIONEER
    "pioneer":                  600,
    "pioner":                   600,  # typo CSV
    "medtronic 5":              600,  # alias en algunos archivos

    # 43 — JL MANUFACTURE
    "jl manufacture":           120,

    # 44 — APLICA
    "aplica":                   40,
    "applica":                  40,

    # 45 — SCHAD 3
    "schad 3":                  40,
    "schad iii":                40,
    "schad 6":                  40,   # alias en algunos archivos

    # 46 — ILUMINACION CIR. II
    "iluminacion cir. ii":      1,
    "iluminacion cir ii":       1,
    "iluminacion circuito ii":  1,
    "iluminacion circ. ii":     1,

    # 47 — ALUMAXINC2
    "alumaxinc2":               80,
    "alumaxinc 2":              80,
    "alumaxin 2":               80,
    "alumaxin ii":              80,
    "alumaxin ll":              80,   # L minúsculas como número

    # 48 — AZODR
    "azodr":                    40,
    "azo dr":                   40,
    "azoor dr":                 40,

    # 49 — JSC FAMILY
    "jsc family":               80,
    "jcs family":               80,

    # 50 — SCHAD IV
    "schad iv":                 40,
    "schad 4":                  40,

    # 51 — ALUMAXINC 3
    "alumaxinc 3":              300,
    "alumaxinc3":               300,
    "alumaxin 3":               300,
    "alumaxin iii":             300,
    "alumaxin lll":             300,  # L minúsculas

    # 52 — INFOTEP
    "infotep":                  1,

    # 53 — LDM
    "ldm":                      240,
    "ldm nuevo":                240,

    # 54 — COOKMEDICAL
    "cookmedical":              80,
    "cook medical":             80,
    "olivar espanol":           80,   # alias en algunos archivos

    # 55 — NATIONAL CHAIN
    "national chain":           160,
    "national 2":               160,  # variante CSV

    # 56 — CONCENTRIX
    "concentrix":               600,
    "stream global":            600,  # alias en algunos archivos

    # 57 — CAJERO LEON
    "cajero leon":              1,

    # 58 — ADUANAS
    "aduanas":                  15,
    "aduana":                   15,

    # 59 — CONSUMO DE OBRA
    "consumo de obra":          1,
    "laurus i":                 1,    # alias en algunos archivos

    # 60 — LAURUS II
    "laurus ii":                240,
    "laurus 2":                 240,
}


def _normalize(name: str) -> str:
    """Normaliza un nombre de empresa para comparación:
    minúsculas, sin puntuación rara, espacios simples."""
    n = name.lower().strip()
    n = re.sub(r"[!@#$%^*_+=\[\]{}|\\\"<>?/]", "", n)
    n = re.sub(r"\s+", " ", n)
    return n


def lookup_default_multiplier(company_name: str) -> int | None:
    """Devuelve el multiplo por defecto para una empresa o None si no se conoce.

    Estrategia (en orden de precisión):
    1. Coincidencia exacta normalizada.
    2. La clave canónica está contenida en el nombre dado (o viceversa),
       siempre que la clave tenga al menos 4 caracteres.
    3. Solapamiento de tokens ≥ 70 %.
    """
    if not company_name:
        return None

    norm = _normalize(company_name)

    # 1. Exacta
    if norm in DEFAULT_MULTIPLIERS:
        return DEFAULT_MULTIPLIERS[norm]

    # 2. Containment
    for key, val in DEFAULT_MULTIPLIERS.items():
        if len(key) >= 4 and (key in norm or norm in key):
            return val

    # 3. Token overlap ≥ 70 %
    norm_tokens = set(norm.split())
    for key, val in DEFAULT_MULTIPLIERS.items():
        key_tokens = set(key.split())
        if not key_tokens:
            continue
        overlap = len(norm_tokens & key_tokens) / max(len(key_tokens), len(norm_tokens))
        if overlap >= 0.70:
            return val

    return None
