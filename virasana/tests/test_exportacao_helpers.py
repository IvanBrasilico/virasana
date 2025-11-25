# virasana/tests/test_exportacao_helpers.py
import io
from datetime import datetime
from typing import Any, Dict, Set, Optional

import pytest
from flask import Flask
from PIL import Image


def _extract_closure_funcs(func, visited_funcs: Optional[Set[int]] = None) -> Dict[str, Any]:
    """
    Percorre recursivamente as closures de uma função para coletar funções internas
    (como _quartis, _cpf_digits etc.). Retorna um dict {__name__: function}.
    Compatível com Python 3.9 (sem operador | em type hints).
    """
    if visited_funcs is None:
        visited_funcs = set()

    out: Dict[str, Any] = {}
    if not hasattr(func, "__code__"):
        return out

    func_id = id(func)
    if func_id in visited_funcs:
        return out
    visited_funcs.add(func_id)

    closure = getattr(func, "__closure__", None)
    freevars = getattr(func.__code__, "co_freevars", ())

    if closure and freevars:
        for name, cell in zip(freevars, closure):
            try:
                val = cell.cell_contents
            except ValueError:
                # célula vazia
                continue

            if callable(val) and hasattr(val, "__name__"):
                out[val.__name__] = val
                out.update(_extract_closure_funcs(val, visited_funcs))
            # Se precisar, aqui dá pra coletar outros valores (dicts, consts) da closure.
    return out


@pytest.fixture(scope="module")
def app_and_helpers():
    """
    Cria um Flask app, chama configure(app) do módulo exportacao_app
    e extrai os helpers puros vasculhando closures das rotas.
    Tenta importar o módulo em dois caminhos comuns ao seu projeto.
    """
    # Import tolerante: raiz (exportacao_app.py) OU dentro de virasana/routes
    mod = None
    import importlib

    for path in ("exportacao_app", "virasana.routes.exportacao_app"):
        try:
            mod = importlib.import_module(path)
            break
        except ImportError:
            continue

    if mod is None:
        raise ImportError(
            "Não consegui importar exportacao_app. "
            "Tente garantir que o arquivo esteja como exportacao_app.py na raiz do PYTHONPATH "
            "ou em virasana/routes/exportacao_app.py."
        )

    app = Flask(__name__)
    # Configs mínimas usadas em configure
    app.config["mongodb"] = {}     # não utilizados pelos helpers aqui
    app.config["db_session"] = None
    # opcional: diretório de thumbs para não tocar em /tmp
    app.config["THUMB_CACHE_DIR"] = "unused_for_helpers"

    # Chama configure (registra rotas e fecha os helpers nas closures)
    mod.configure(app)

    vf = app.view_functions
    candidates = []
    # rotas que "conectam" às closures que queremos
    if "transit_time" in vf:
        candidates.append(vf["transit_time"])
    if "exportacao_transit_time_imgs_bulk" in vf:
        candidates.append(vf["exportacao_transit_time_imgs_bulk"])
    if "exportacao_img" in vf:
        candidates.append(vf["exportacao_img"])

    helpers: Dict[str, Any] = {}
    for f in candidates:
        helpers.update(_extract_closure_funcs(f))

    required = {
        "_quartis",
        "_cpf_digits",
        "_sanitize_origens",
        "_normaliza_destino",
        "_parse_local_to_utc_naive",
        "_make_thumb_bytes",
    }
    missing = required - set(helpers.keys())
    assert not missing, f"Não encontrei helpers: {missing}. Helpers vistos: {sorted(helpers.keys())}"

    return app, helpers


# -----------------------------
# _quartis(sorted_vals)
# -----------------------------
@pytest.mark.parametrize(
    "vals,expected",
    [
        ([], (None, None)),
        ([1], (None, None)),               # Tukey: exclui a mediana em listas ímpares
        ([1, 2], (1.0, 2.0)),
        ([1, 2, 3], (1.0, 3.0)),
        ([1, 1, 1, 1], (1.0, 1.0)),
        ([1, 2, 3, 4], (1.5, 3.5)),
        ([10.0, 20.0, 30.0, 40.0, 50.0], (15.0, 45.0)),
    ],
)
def test_quartis_tukey(vals, expected, app_and_helpers):
    _, H = app_and_helpers
    q1, q3 = H["_quartis"](sorted(vals))
    assert (q1, q3) == expected


# -----------------------------
# _cpf_digits(s)
# -----------------------------
@pytest.mark.parametrize(
    "s,expected",
    [
        (None, ""),
        ("", ""),
        ("123.456.789-09", "12345678909"),
        ("  111.222.333-44  ", "11122233344"),
        ("00000000000", "00000000000"),
        ("12 345 678 901", "12345678901"),
    ],
)
def test_cpf_digits(s, expected, app_and_helpers):
    _, H = app_and_helpers
    assert H["_cpf_digits"](s) == expected


# -----------------------------
# _sanitize_origens(lst)
# -----------------------------
def test_sanitize_origens_filters_and_preserves_order(app_and_helpers):
    _, H = app_and_helpers
    _sanitize_origens = H["_sanitize_origens"]
    # 8932793, 8932761 e 8931356 são válidos em ORIGENS_OPCOES
    entrada = ["8932793", "xpto", "8932761", "foo", "8931356"]
    saida = _sanitize_origens(entrada)
    # Deve manter somente válidos e na mesma ordem
    assert saida == ["8932793", "8932761", "8931356"]
    # Itens inválidos não podem aparecer
    assert "xpto" not in saida
    assert "foo" not in saida


@pytest.mark.parametrize(
    "entrada,saida",
    [
        (None, []),
        ([], []),
        (["invalido"], []),
    ],
)
def test_sanitize_origens_none_empty_invalid(entrada, saida, app_and_helpers):
    _, H = app_and_helpers
    assert H["_sanitize_origens"](entrada) == saida


# -----------------------------
# _normaliza_destino(valor)
# -----------------------------
@pytest.mark.parametrize(
    "valor,esperado",
    [
        ("8931356", "8931356"),
        ("8931359", "8931359"),
        ("8931404", "8931404"),
        ("8931318", "8931318"),
        ("qualquer", "8931356"),
        (None, "8931356"),
        ("", "8931356"),
    ],
)
def test_normaliza_destino(valor, esperado, app_and_helpers):
    _, H = app_and_helpers
    assert H["_normaliza_destino"](valor) == esperado


# -----------------------------
# _parse_local_to_utc_naive(s)
# -----------------------------
def test_parse_local_to_utc_naive_standard_conversion(app_and_helpers):
    _, H = app_and_helpers
    f = H["_parse_local_to_utc_naive"]

    dt_local_str = "2025-06-01 12:00:00"  # meio-dia em São Paulo (UTC-3 na data)
    dt = f(dt_local_str)
    assert dt.tzinfo is None
    assert dt == datetime(2025, 6, 1, 15, 0, 0)  # +3h


@pytest.mark.parametrize("bad", ["2025-06-01", "2025/06/01 12:00", "foo", "", None])
def test_parse_local_to_utc_naive_invalid_raises(bad, app_and_helpers):
    _, H = app_and_helpers
    f = H["_parse_local_to_utc_naive"]
    with pytest.raises(Exception):
        f(bad)  # deve falhar no strptime


# -----------------------------
# _make_thumb_bytes(img_bytes, width)
# -----------------------------
@pytest.mark.parametrize("target_w", [64, 128, 320, 1000])
def test_make_thumb_bytes_generates_jpeg_and_respects_width(target_w, app_and_helpers):
    _, H = app_and_helpers
    f = H["_make_thumb_bytes"]

    # cria imagem RGB 800x600
    img = Image.new("RGB", (800, 600), (200, 120, 80))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    raw = buf.getvalue()

    out = f(raw, target_w)
    assert isinstance(out, (bytes, bytearray))
    assert len(out) > 0

    out_img = Image.open(io.BytesIO(out))
    assert out_img.format == "JPEG"
    w, h = out_img.size
    assert max(w, h) <= target_w
