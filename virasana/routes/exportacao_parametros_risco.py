import logging
from flask import Blueprint, render_template, request, jsonify, current_app
from sqlalchemy import text
from flask_wtf.csrf import generate_csrf
from flask_login import login_required, current_user

# Criação do Blueprint
parametros_risco_bp = Blueprint(
    'parametros_risco',
    __name__,
    url_prefix='/exportacao/parametros_risco'
)

@parametros_risco_bp.route('/', methods=['GET'])
@login_required
def index():
    session = current_app.config['db_session']
    
    # Busca os portos cadastrados (ajuste os nomes das colunas conforme sua tabela)
    try:
        sql_portos = text("""
            SELECT id, codigo_porto, nome_porto, peso_risco 
            FROM narcos_risco_portos 
            ORDER BY nome_porto
        """)
        portos = session.execute(sql_portos).mappings().all()
    except Exception as e:
        current_app.logger.exception("[parametros_risco] Erro ao buscar portos")
        portos = []

    # Aqui você pode adicionar outras consultas (NCMs, CNPJs, etc.)
    # ncms = session.execute(text("SELECT ... FROM narcos_risco_ncms")).mappings().all()

    return render_template(
        'exportacao_parametros_risco.html',
        csrf_token=generate_csrf,
        portos=portos
        # ncms=ncms
    )

@parametros_risco_bp.route('/atualizar', methods=['POST'])
@login_required
def atualizar_peso():
    """
    Endpoint genérico para atualizar o peso via AJAX.
    Espera um JSON: {"tabela": "portos", "id": 123, "peso_risco": 5.5}
    """
    payload = request.get_json(silent=True) or {}
    tabela_req = payload.get("tabela")
    registro_id = payload.get("id")
    novo_peso = payload.get("peso_risco")

    if not all([tabela_req, registro_id, novo_peso is not None]):
        return jsonify({"error": "Dados incompletos fornecidos."}), 400

    # Mapeamento de segurança para evitar SQL Injection no nome da tabela
    tabelas_permitidas = {
        'portos': 'narcos_risco_portos',
        # 'ncms': 'narcos_risco_ncms',
        # 'transportadoras': 'narcos_risco_transportadoras'
    }

    if tabela_req not in tabelas_permitidas:
        return jsonify({"error": "Tabela não autorizada."}), 403

    nome_tabela_real = tabelas_permitidas[tabela_req]
    session = current_app.config['db_session']

    try:
        # Atualiza o registro. Ajuste o nome da coluna 'peso_risco' e 'id' se necessário.
        sql_update = text(f"""
            UPDATE {nome_tabela_real} 
            SET peso_risco = :peso 
            WHERE id = :id
        """)
        session.execute(sql_update, {"peso": float(novo_peso), "id": registro_id})
        session.commit()
        
        current_app.logger.info(f"[parametros_risco] Usuário {current_user.id} alterou {tabela_req} ID {registro_id} para peso {novo_peso}")
        return jsonify({"status": "success", "message": "Peso atualizado com sucesso."})

    except Exception as e:
        session.rollback()
        current_app.logger.exception(f"[parametros_risco] Erro ao atualizar tabela {nome_tabela_real}")
        return jsonify({"error": "Erro interno ao salvar no banco de dados."}), 500