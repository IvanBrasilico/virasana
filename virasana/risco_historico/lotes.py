# coding: utf-8
"""
Lotes do risco histórico por CE pela linha de comando (no F3 entram no laço do
serviço do ciclo de vida). Ver virasana/risco_historico/CONTEXTO.md.

  --sinais      extração dos sinais dos CEs (lote 2a). Na primeira vez grava a
                marca inicial: CEs emitidos nos últimos --desde-dias (padrão 30)
                ou a partir de --desde-id. Depois é incremental. Repete as voltas
                enquanto houver fila (--uma-vez: só uma).
  --parametros  nova versão dos parâmetros (lote 1), se as fichas positivas
                mudaram ou a última venceu (--forca: sempre).
  --situacao    marcas e quanto falta.
  --simula      cada volta numa transação desfeita no fim: mede sem gravar (com
                --sinais, faz uma volta só).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    LOTES=virasana/risco_historico/lotes.py
    virasana-venv/bin/python $LOTES --sinais --simula           # mede uma volta (e a marca inicial)
    virasana-venv/bin/python $LOTES --sinais                    # carga inicial, até alcançar o Mercante
    virasana-venv/bin/python $LOTES --parametros                # versão dos parâmetros
    virasana-venv/bin/python $LOTES --homologacao --situacao
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.risco_historico import parametros  # noqa: E402
from virasana.risco_historico.extracao import ErroInicio, Extracao  # noqa: E402

logger = logging.getLogger('risco_historico.lotes')


def uri_do_banco(args, ambiente=os.environ) -> str:
    """--uri explícita; --homologacao usa SQL_URI_HOM (como o serviço do ciclo); senão, SQL_URI."""
    if args.uri:
        return args.uri
    if args.homologacao:
        return ambiente['SQL_URI_HOM']
    from ajna_commons.flask.conf import SQL_URI
    return SQL_URI


def _transacao(engine, simula: bool, funcao):
    """Roda `funcao(conexao)` numa transação; desfaz se for simulação."""
    with engine.connect() as conexao:
        transacao = conexao.begin()
        try:
            resultado = funcao(conexao)
        except Exception:
            transacao.rollback()
            raise
        if simula:
            transacao.rollback()
        else:
            transacao.commit()
        return resultado


def sinais(engine, args, relogio=datetime.now) -> int:
    """Voltas da extração; devolve quantas fez."""
    def volta(conexao):
        extracao = Extracao(conexao, relogio, lote_ces=args.lote)
        inicio = extracao.inicia(args.desde_dias, args.desde_id)
        if inicio:
            logger.info(f'marcas iniciais: {inicio}')
        return extracao.uma_volta()

    voltas = 0
    while True:
        comeco = time.perf_counter()
        estatisticas, cheio = _transacao(engine, args.simula, volta)
        voltas += 1
        logger.info(f'volta {voltas} em {time.perf_counter() - comeco:.1f} s'
                    f'{" (SIMULAÇÃO: nada gravado)" * args.simula}: {dict(estatisticas)}')
        if not cheio or args.uma_vez or args.simula:
            return voltas


def main(argv=None):  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--sinais', action='store_true', help='extração dos sinais dos CEs (lote 2a)')
    parser.add_argument('--parametros', action='store_true', help='nova versão dos parâmetros (lote 1), se preciso')
    parser.add_argument('--situacao', action='store_true', help='marcas e quanto falta')
    parser.add_argument('--forca', action='store_true', help='--parametros: nova versão mesmo sem mudança')
    parser.add_argument('--simula', action='store_true', help='desfaz no fim: mede sem gravar')
    parser.add_argument('--uma-vez', action='store_true', help='--sinais: uma volta só')
    parser.add_argument('--desde-dias', type=int, default=30, help='marca inicial: CEs emitidos nos últimos N dias')
    parser.add_argument('--desde-id', type=int, help='marca inicial: a partir deste conhecimentosresumo.ID')
    parser.add_argument('--lote', type=int, default=10000, help='--sinais: CEs novos por volta (padrão 10000)')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s', stream=sys.stdout)
    engine = create_engine(uri_do_banco(args))
    try:
        if args.sinais:
            sinais(engine, args)
        if args.parametros:
            versao, motivo = _transacao(engine, args.simula,
                                        lambda conexao: parametros.atualiza(conexao, datetime.now(), args.forca))
            logger.info(f'parâmetros: {motivo}' + (f' -> versão {versao}' if versao else '')
                        + (' (SIMULAÇÃO: nada gravado)' if args.simula and versao else ''))
        if args.situacao:
            with engine.connect() as conexao:
                extracao = Extracao(conexao)
                logger.info(f'marcas: {extracao.marcas()}')
                logger.info(f'situação: {extracao.situacao()}')
                logger.info(f'última versão dos parâmetros: {parametros.ultima_versao(conexao)}')
    except ErroInicio as erro:
        logger.error(str(erro))
        sys.exit(1)


if __name__ == '__main__':  # pragma: no cover
    main()
