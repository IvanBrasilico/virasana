# coding: utf-8
"""
HEALTH CHECK diário do NAVI — OK / ATENÇÃO / FALHA por componente (plano em `BACKLOG.md`, 23/10/2026).

Componentes, na ordem do relatório: fontes de dados (Mercante, API Recintos, imagens no MongoDB), ciclo de vida
(serviço, invariantes, refaz, cobertura), pesagens, risco histórico, busca por texto, NAVI-I e Fichas de FMA. O
detalhe de cada medida e os limiares estão nos módulos de `virasana/saude/`. Só leitura; cada consulta SQL com limite
de tempo; uma verificação que falha (banco fora, tabela ausente) vira FALHA dela e as outras seguem.

A rodada diária (7 h) e as pedidas pela tela `/saude/` (perfil Supervisor) ficam com o SERVIÇO `servico_saude.py`
(programa `saude_navi` do supervisor), que grava o histórico no Mongo (`historico.py`). Esta linha de comando é para
rodar à mão: sai com 0 (tudo OK), 1 (alguma ATENÇÃO) ou 2 (alguma FALHA); com `--grava`, a rodada entra no histórico
(origem "manual").

Uso (no servidor, na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons — SQL_URI, MONGODB_URI
— e, para medir a busca por texto, NAVII_MEILI_CHAVE_ESCRITA):
    S=virasana/saude/saude_navi.py
    virasana-venv/bin/python $S                                   # na tela
    virasana-venv/bin/python $S --saida /var/log/virasana/saude/saude_$(date +%Y%m%d).log
    virasana-venv/bin/python $S --nucleo-desde 2026-10-22         # depois de trocar o núcleo do ciclo de vida
    virasana-venv/bin/python $S --so mercante api imagens         # só algumas verificações
    virasana-venv/bin/python $S --grava                           # e grava no histórico da tela
"""
import argparse
import os
import sys
from datetime import datetime
from typing import List

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.saude import aplicacoes, ciclo, fontes, imagens  # noqa: E402
from virasana.saude.verificacao import Contexto, Resultado, codigo_de_saida, executa, relatorio  # noqa: E402

# (atalho do --so, componente, verificação)
VERIFICACOES = (
    ('mercante', fontes.MERCANTE, fontes.verifica_mercante),
    ('api', fontes.API, fontes.verifica_api_recintos),
    ('imagens', imagens.IMAGENS, imagens.verifica_imagens),
    ('servico', ciclo.SERVICO, ciclo.verifica_servico),
    ('invariantes', ciclo.INVARIANTES, ciclo.verifica_invariantes),
    ('refaz', ciclo.REFEITOS, ciclo.verifica_refeitos),
    ('cobertura', ciclo.COBERTURA, ciclo.verifica_cobertura),
    ('pesagens', ciclo.PESAGENS, ciclo.verifica_pesagens),
    ('risco', aplicacoes.RISCO, aplicacoes.verifica_risco),
    ('busca', aplicacoes.BUSCA, aplicacoes.verifica_busca),
    ('navii', aplicacoes.NAVII, aplicacoes.verifica_navii),
    ('fma', aplicacoes.FMA, aplicacoes.verifica_fma),
)
LIMITE_SQL_SEGUNDOS = 120


def roda(contexto: Contexto, so=None) -> List[Resultado]:
    return [executa(componente, verificacao, contexto) for atalho, componente, verificacao in VERIFICACOES
            if not so or atalho in so]


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--sem-mongo', action='store_true', help='não mede imagens nem o NAVI-I')
    parser.add_argument('--nucleo-desde', type=lambda v: datetime.strptime(v, '%Y-%m-%d'), metavar='AAAA-MM-DD',
                        help='última troca do núcleo do ciclo de vida: mede os ciclos ainda não refeitos')
    parser.add_argument('--so', nargs='+', choices=[atalho for atalho, _, _ in VERIFICACOES], metavar='VERIFICAÇÃO',
                        help='só estas: ' + ', '.join(atalho for atalho, _, _ in VERIFICACOES))
    parser.add_argument('--saida', help='grava o relatório neste arquivo (além da tela)')
    parser.add_argument('--grava', action='store_true', help='grava a rodada no histórico (tela /saude/)')
    return parser.parse_args(argv)


def recursos(args):  # pragma: no cover - depende dos bancos reais
    """(engine com limite de tempo, banco do Mongo do virasana ou None, cliente do Meilisearch ou None) — a linha de
    comando e o serviço usam os mesmos."""
    from sqlalchemy import create_engine
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    from virasana.navii import texto_navii as tn
    from virasana.navii.mercante_por_id import limita_tempo
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    limita_tempo(engine, LIMITE_SQL_SEGUNDOS)
    mongo = None
    if not args.sem_mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo = MongoClient(host=MONGODB_URI, serverSelectionTimeoutMS=20000)[DATABASE]
    meili = tn.cliente_do_ambiente(tn.VARIAVEL_ESCRITA) if os.environ.get(tn.VARIAVEL_ESCRITA) else None
    return engine, mongo, meili


def main(argv=None):  # pragma: no cover - depende dos bancos reais
    args = _argumentos(argv)
    engine, mongo, meili = recursos(args)
    contexto = Contexto(engine, mongo=mongo, meili=meili, nucleo_desde=args.nucleo_desde)
    resultados = roda(contexto, args.so)
    linhas = relatorio(resultados, contexto.agora)
    print('\n'.join(linhas))
    if args.saida:
        with open(args.saida, 'w', encoding='utf-8') as arquivo:
            arquivo.write('\n'.join(linhas) + '\n')
    if args.grava and mongo is not None:
        from virasana.saude.historico import grava_rodada
        grava_rodada(mongo, resultados, contexto.agora, datetime.now(), 'manual', os.environ.get('USER'))
    sys.exit(codigo_de_saida(resultados))


if __name__ == '__main__':  # pragma: no cover
    main()
