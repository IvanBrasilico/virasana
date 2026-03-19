import logging
import unicodedata
import re
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

def normalizar_chave(texto):
    """
    Remove acentos, espaços, pontuações e converte para maiúsculo.
    Ex: " Antuérpia - BE " -> "ANTUERPIABE"
    """
    if not texto:
        return ""
    
    # Converte para string e maiúsculo
    texto = str(texto).upper()
    # Remove acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    # Remove tudo que não for letra ou número (espaços, hífens, pontos, etc)
    texto = re.sub(r'[^A-Z0-9]', '', texto)
    
    return texto

def parse_booleano(valor):
    """
    Avalia se o valor do alerta_if significa 'Verdadeiro'.
    Trata variações comuns vindas de planilhas de Excel.
    """
    if valor is None:
        return False
    if isinstance(valor, bool):
        return valor
    
    val_str = str(valor).strip().upper()
    return val_str in ('TRUE', '1', 'SIM', 'S', 'X', 'YES', 'Y')

def obter_pesos_ativos(session):
    """
    Busca os pesos configurados no banco de dados.
    Retorna um dicionário: {'CRITERIO': peso_float}
    """
    sql = text("""
        SELECT criterio, peso 
        FROM narcos_risco_pesos_config 
        WHERE ativo = 1
    """)
    rows = session.execute(sql).mappings().all()
    
    # Se a tabela estiver vazia, retorna pesos padrão de segurança
    if not rows:
        return {
            'PORTO_DESCARGA': 1.0,
            'PORTO_DESTINO_FINAL': 1.0,
            'ALERTA_IF': 1.0
        }
        
    return {r['criterio'].upper(): float(r['peso']) for r in rows}

def processar_lote_risco(session, registros_planilha):
    """
    Calcula e salva o risco para uma lista de registros.
    Ideal para ser chamado após o import da planilha.
    
    registros_planilha: lista de dicionários contendo as chaves 
    'numero_conteiner', 'porto_descarga', 'porto_destino_final', 'alerta_if'
    """
    if not registros_planilha:
        return 0

    pesos = obter_pesos_ativos(session)
    peso_pd = pesos.get('PORTO_DESCARGA', 0.0)
    peso_pdf = pesos.get('PORTO_DESTINO_FINAL', 0.0)
    peso_alerta = pesos.get('ALERTA_IF', 0.0)
    
    peso_transp = pesos.get('TRANSPORTADORA', 0.0)
    
    soma_pesos = peso_pd + peso_pdf + peso_alerta + peso_transp
    
    if soma_pesos == 0:
        logger.warning("[score_risco] Soma dos pesos é zero. Verifique a configuração.")
        return 0

    # ------------------------------------------------------------------
    # 1 e 2. Buscar TODOS os portos e criar o mapa normalizado em memória
    # Como não temos mais a 'chave_busca' no banco, fazemos o match no Python.
    # Isso é extremamente performático pois a lista de portos de risco é pequena.
    # ------------------------------------------------------------------
    mapa_notas_portos = {}
    try:
        sql_portos = text("""
            SELECT nome_porto_original, nota_risco 
            FROM narcos_risco_portos
        """)
        rows_portos = session.execute(sql_portos).mappings().all()
        
        for r in rows_portos:
            # Pega o nome "Rotterdam (Holanda)" e transforma em "ROTTERDAMHOLANDA"
            chave_banco = normalizar_chave(r['nome_porto_original'])
            mapa_notas_portos[chave_banco] = float(r['nota_risco'])
            
    except SQLAlchemyError as e:
        logger.exception("[score_risco] Erro ao buscar tabela de portos.")
        return 0 # Aborta se não conseguir ler as matrizes de risco

    # ------------------------------------------------------------------
    # 2.5. Buscar TODAS as transportadoras e criar mapas em memória
    # Cria dois índices: um por CNPJ (prioridade) e um por Nome (fallback)
    # ------------------------------------------------------------------
    mapa_notas_transp_cnpj = {}
    mapa_notas_transp_nome = {}
    try:
        sql_transp = text("""
            SELECT nome_transportadora, cnpj_transportadora, nota_risco 
            FROM narcos_risco_transportadoras
        """)
        rows_transp = session.execute(sql_transp).mappings().all()
        for r in rows_transp:
            cnpj_norm = normalizar_chave(r['cnpj_transportadora'])
            nome_norm = normalizar_chave(r['nome_transportadora'])
            nota = float(r['nota_risco'])
            if cnpj_norm: mapa_notas_transp_cnpj[cnpj_norm] = nota
            if nome_norm: mapa_notas_transp_nome[nome_norm] = nota
    except SQLAlchemyError as e:
        logger.exception("[score_risco] Erro ao buscar tabela de transportadoras.")
        return 0

    # 3. Calcular o risco para cada contêiner
    resultados_para_salvar = []
    
    for reg in registros_planilha:
        numero_conteiner = reg.get('numero_conteiner')
        if not numero_conteiner:
            continue
            
        # Notas individuais (Zero se não encontrado/vazio)
        chave_pd = normalizar_chave(reg.get('porto_descarga'))
        nota_pd = mapa_notas_portos.get(chave_pd, 0.0)
        
        chave_pdf = normalizar_chave(reg.get('porto_destino_final'))
        nota_pdf = mapa_notas_portos.get(chave_pdf, 0.0)
        
        # Alerta IF: 10 se verdadeiro, 0 se falso
        tem_alerta = parse_booleano(reg.get('alerta_if'))
        nota_alerta = 10.0 if tem_alerta else 0.0

        # Transportadora: Tenta por CNPJ primeiro, depois faz fallback pelo Nome
        cnpj_original = reg.get('cnpj_transportadora')
        nome_original = reg.get('transportadora')
        chave_cnpj = normalizar_chave(cnpj_original)
        chave_nome = normalizar_chave(nome_original)
        
        nota_transp = 0.0
        if chave_cnpj and chave_cnpj in mapa_notas_transp_cnpj:
            nota_transp = mapa_notas_transp_cnpj[chave_cnpj]
        elif chave_nome and chave_nome in mapa_notas_transp_nome:
            nota_transp = mapa_notas_transp_nome[chave_nome]

        # Média ponderada
        soma_notas = (nota_pd * peso_pd) + (nota_pdf * peso_pdf) + (nota_alerta * peso_alerta) + (nota_transp * peso_transp)
        nota_final = soma_notas / soma_pesos

        # Montar a memória de cálculo (JSON)
        memoria = {
            "pesos_utilizados": pesos,
            "criterios": {
                "porto_descarga": {"valor_original": reg.get('porto_descarga'), "chave": chave_pd, "nota": nota_pd, "peso": peso_pd},
                "porto_destino_final": {"valor_original": reg.get('porto_destino_final'), "chave": chave_pdf, "nota": nota_pdf, "peso": peso_pdf},
                "alerta_if": {"valor_original": reg.get('alerta_if'), "nota": nota_alerta, "peso": peso_alerta},
                "transportadora": {"cnpj_original": cnpj_original, "nome_original": nome_original, "nota": nota_transp, "peso": peso_transp}
            }
        }

        resultados_para_salvar.append({
            "numero_conteiner": str(numero_conteiner).strip().upper(),
            "nota_final": round(nota_final, 2),
            "memoria_calculo": json.dumps(memoria, ensure_ascii=False)
        })

    # 4. Salvar no banco de dados (Insert on Duplicate Key Update)
    if resultados_para_salvar:
        try:
            sql_insert = text("""
                INSERT INTO narcos_risco_calculado (numero_conteiner, nota_final, memoria_calculo, data_calculo)
                VALUES (:numero_conteiner, :nota_final, :memoria_calculo, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE 
                    nota_final = VALUES(nota_final),
                    memoria_calculo = VALUES(memoria_calculo),
                    data_calculo = CURRENT_TIMESTAMP
            """)
            session.execute(sql_insert, resultados_para_salvar)
            session.commit()
            return len(resultados_para_salvar)
        except SQLAlchemyError as e:
            session.rollback()
            logger.exception("[score_risco] Erro ao salvar notas calculadas.")
            raise e

    return 0