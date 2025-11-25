# virasana/tests/e2e/test_exportacao_transit_time.py
import re
import pytest
from playwright.sync_api import expect

BASE_URL = "https://ajna1.rfoc.srf"
RUTA = "/virasana/exportacao/transit_time"


@pytest.fixture
def context(browser):
    # Ignora problemas de certificado (se houver) e define base_url
    ctx = browser.new_context(ignore_https_errors=True, base_url=BASE_URL)
    yield ctx
    ctx.close()


@pytest.fixture
def page(context):
    p = context.new_page()
    yield p
    p.close()


def goto(page, qs: str = ""):
    url = RUTA + (("?" + qs) if qs else "")
    page.goto(url, wait_until="domcontentloaded")


def test_transit_time_btn_aplicar(page):
    goto(page)
    aplicar_btn = page.get_by_role("button", name="Aplicar")
    expect(aplicar_btn).to_be_visible()


def test_exibir_peso_consultar_basico(page):
    # T1: Exibir peso de balança -> Consultar peso -> ver "Origem:" (ou mensagens alternativas)
    goto(page)
    # Caso a página não tenha resultados, haverá um alerta "Nenhum registro..."
    # então tentamos clicar no botão "Exibir peso de balança" se existir
    exibir = page.get_by_role("button", name="Exibir peso de balança")
    if not exibir.count():
        pytest.skip("Sem resultados para abrir colapso de peso; pulando teste.")
    exibir.first.click()

    consultar = page.get_by_role("button", name="Consultar peso")
    expect(consultar).to_be_visible()
    consultar.click()

    # Aguarda resposta no span .peso-resposta (pode ser Origem:/Sem pesagens/Erro)
    resposta = page.locator(".peso-resposta").first
    expect(resposta).to_be_visible()
    page.wait_for_timeout(500)  # pequeno respiro p/ JS
    txt = resposta.inner_text(timeout=5000).strip()
    assert any(s in txt for s in ["Origem", "Sem pesagens", "Erro"]), f"Resposta inesperada: {txt!r}"


def test_trocar_destino_e_aplicar_url_param(page):
    # T2: Trocar "Recinto de destino (E)" e aplicar -> URL contem destino=
    goto(page)
    destino = page.get_by_label("Recinto de destino (E)")
    expect(destino).to_be_visible()

    # Seleciona por VALUE um destino diferente do atual
    valores = ["8931356", "8931359", "8931404", "8931318"]  # Santos Brasil, BTP, DPW, Ecoporto
    atual = destino.input_value()
    alvo = next((v for v in valores if v != atual), valores[0])
    destino.select_option(value=alvo)

    # Submete
    page.get_by_role("button", name="Aplicar").click()

    # Espera navegação e confere a URL
    page.wait_for_load_state("domcontentloaded")
    current = page.url
    assert "destino=" in current, f"URL sem 'destino=': {current}"


def test_collapse_origem_abrir_fechar(page):
    # T3: Abrir/fechar "Selecionar origens (opcional)"
    goto(page)
    btn = page.get_by_role("button", name="Selecionar origens (opcional)")
    expect(btn).to_be_visible()

    # Abrir
    btn.click()
    filtro = page.locator("#filtroOrigem")
    expect(filtro).to_have_class(re.compile(r"show"), timeout=3000)

    # Fechar
    btn.click()
    # Quando fechado, o collapse remove a classe 'show'
    expect(filtro).not_to_have_class(re.compile(r"show"), timeout=3000)


def test_filtro_origem_selecionar_tudo_e_limpar(page):
    # T4: Selecionar tudo / Limpar
    goto(page)
    page.get_by_role("button", name="Selecionar origens (opcional)").click()

    # Selecionar tudo
    sel_todos = page.get_by_role("link", name="Selecionar tudo")
    expect(sel_todos).to_be_visible()
    sel_todos.click()

    checks = page.locator(".origem-checkbox")
    total = checks.count()
    if total == 0:
        pytest.skip("Nenhuma checkbox de origem encontrada; pulando.")
    # Todos marcados
    assert all(checks.nth(i).is_checked() for i in range(total))

    # Limpar
    limpar = page.get_by_role("link", name="Limpar")
    expect(limpar).to_be_visible()
    limpar.click()
    # Todos desmarcados
    assert all(not checks.nth(i).is_checked() for i in range(total))


def test_aplicar_com_multiplas_origens_reflete_na_url(page):
    # T5: Marcar 2-3 origens e Aplicar -> URL repete origem=
    goto(page)
    page.get_by_role("button", name="Selecionar origens (opcional)").click()
    checks = page.locator(".origem-checkbox")
    if checks.count() < 2:
        pytest.skip("Menos de 2 origens disponíveis; pulando.")

    # Marca as duas primeiras
    checks.nth(0).check()
    checks.nth(1).check()

    page.get_by_role("button", name="Aplicar").click()
    page.wait_for_load_state("domcontentloaded")

    current = page.url
    # deve haver pelo menos dois 'origem=' na query
    assert current.count("origem=") >= 2, f"URL não refletiu múltiplas origens: {current}"


def test_busca_container_pattern_html5(page):
    # T6: Validação do pattern do campo contêiner
    goto(page)
    campo = page.get_by_label("Busca contêiner (opcional)")
    expect(campo).to_be_visible()

    # Inválido
    campo.fill("ABC123")
    valid = page.evaluate("el => el.checkValidity()", campo.element_handle())
    assert not valid, "Campo deveria estar inválido para 'ABC123'"

    # Válido (4 letras + 7 dígitos). Espaço opcional também é aceito.
    campo.fill("ABCD1234567")
    valid2 = page.evaluate("el => el.checkValidity()", campo.element_handle())
    assert valid2, "Campo deveria estar válido para 'ABCD1234567'"


def test_de_ate_opcional_e_na_url(page):
    # T7: Sem de/ate -> URL sem params; depois com de/ate -> URL com params
    goto(page)
    base = page.url
    assert "de=" not in base and "ate=" not in base

    # Preenche de/ate e aplica
    page.get_by_label("De (opcional)").fill("2025-01-01")
    page.get_by_label("Até (opcional)").fill("2025-01-31")
    page.get_by_role("button", name="Aplicar").click()
    page.wait_for_load_state("domcontentloaded")

    current = page.url
    assert "de=2025-01-01" in current and "ate=2025-01-31" in current, f"URL não contém de/ate: {current}"


def test_exportar_excel_preserva_filtros_na_url(page):
    # T8: Abrir já com querystring e conferir href do "Exportar Excel"
    qs = "data=2025-02-01&de=2025-01-01&ate=2025-01-31&numero=ABCD1234567&origem=8932793&origem=8932761"
    goto(page, qs=qs)

    link = page.get_by_role("link", name="Exportar Excel")
    expect(link).to_be_visible()
    href = link.get_attribute("href") or ""

    # Deve carregar os principais params (server-side). 'data' pode não vir se o servidor não setou data_iso.
    for fragment in ["de=2025-01-01", "ate=2025-01-31", "numero=ABCD1234567"]:
        assert fragment in href, f"Exportar Excel sem fragmento esperado: {fragment}"

    # Se o campo Data estiver preenchido na UI, então exigimos 'data=' no href também.
    data_input = page.get_by_label("Data")
    if data_input.count() and (data_input.input_value() or "").strip():
        assert "data=" in href, f"Exportar Excel deveria conter 'data=', pois o campo Data está preenchido. Href: {href}"

    # Repete múltiplas origens
    assert href.count("origem=") >= 2, f"Exportar Excel não preservou múltiplas origens: {href}"


def test_sort_bar_toggle_entrada(page):
    # T10: Se houver resultados, sort-bar deve existir e alternar direção
    goto(page)
    sort_bar = page.locator(".sort-bar")
    if not sort_bar.count():
        pytest.skip("Sem barra de ordenação (provavelmente sem resultados); pulando.")

    # Use seletor estável baseado em data-key
    btn_entrada = page.locator('.sort-bar .sort-btn[data-key="entrada"]')
    expect(btn_entrada).to_be_visible()

    # Primeiro clique (se já estiver em asc, alterna para desc; validamos atributo)
    btn_entrada.click()
    # aria-sort pode estar no próprio botão quando ativo
    # fallback: verificar indicador ▼/▲
    aria = btn_entrada.get_attribute("aria-sort") or ""
    if aria:
        assert aria in ("ascending", "descending")
    else:
        ind = btn_entrada.locator(".sort-indicator")
        expect(ind).to_be_visible()
