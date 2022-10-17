import logging
from virasana.main import app

if __name__ == '__main__':
    app.run()
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
"""
NOME_SCRIPT = "ANR xVIG EXPORTAÇÃO";
csrf_token = null;
run();

function
run()
{

if (!checaProjeto()) {
return;}

if (hasNewerVersion()) {
if (atualizaVersao()) {
return;}
}

janelas.mostraMenuComSubrotinas(
    [
"1.1. DUEs - Importar por período", "menuDetalhaDUEs",
"1.2. Abrir MAD - DUEs detalhadas", "menuMadDUEs",
"2.1. Cargas para exportação (ainda sem DUE) - Importar por período", "menuDetalhaEstoqueDUEs",
"2.2. Abrir MAD - Estoque de Carga", "menuMadEstoqueDUEs",
],
"Menu ANR Exportação",
true
);

janelas.mostraMensagem("Fim da execução");
}

function
checaProjeto()
{
    var
prj = contagil.getProjetoAtual();
if (!prj)
{
    janelas.mostraErro("Você precisa estar no contexto de um PROJETO!");
throw
"No Project";
}
return true;
}

function
atualizaVersao()
{
var
resposta = janelas.pedeConfirmacao("Versão desatualizada. Deseja atualizar?")
if (resposta) {
contagil.baixaScript(NOME_SCRIPT)
janelas.mostraMensagem("Inicie novamente o script")
return true;
}

return false;
}

function
hasNewerVersion(versao_online, versao_local)
{
try {
var versao_atual = contagil.getVersaoScript(NOME_SCRIPT);
var versao_online = null;

try {
versao_online = contagil.getVersaoScriptServidor(NOME_SCRIPT);
} catch(e) {
println("Erro ao checar versão: " + e);
}

if (versao_online) {
versao_atual = versao_atual.split(".")
versao_online = versao_online.split(".")

if (parseInt(versao_online[0]) > parseInt(versao_atual[0])) {
return true;
} else if (parseInt(versao_online[1]) > parseInt(versao_atual[1])) {
return true;
} else if (parseInt(versao_online[2]) > parseInt(versao_atual[2])) {
return true;
}
}

} catch(e)
{
    println("Erro ao checar versão: " + e);
return true;
}

return false;
}



function
menuDetalhaDUEs()
{
var
periodo = pedePeriodo();
var
cod_rf_embarques = pedeLocaisEmbarque();
var
acrescenta = pedeAcrescentar();
detalhaDUEs(periodo, cod_rf_embarques, acrescenta);
}

function
menuDetalhaEstoqueDUEs()
{
var
periodo = pedePeriodo();
var
cod_urfs = pedeLocaisEmbarque();
var
acrescenta = pedeAcrescentar();
detalhaEstoqueDUEs(periodo, cod_urfs, acrescenta);
}

function
menuMadDUEs()
{
contagil.getMAD(MAD_DUES, true);
}

function
menuMadEstoqueDUEs()
{
contagil.getMAD(MAD_ESTOQUE_CARGA, true);
}


function
pedeAcrescentar()
{
return janelas.pedeConfirmacao(
    "Deseja adicionar os resultados às tabelas já existentes?\nResponder 'NÃO' irá sobrescrever os dados.")
}

function
pedePeriodo()
{
var
hoje = contagil.getDataAtual();
var
ontem = hoje.somaDias(-1);
var
data_limite = hoje.somaDias(-90);

var
periodo = janelas.pedePeriodo("Indique um período para consulta:", ontem, hoje);
var
data_inicio = periodo.getDataInicial();
var
data_fim = periodo.getDataFinal();

if (data_inicio.after(data_fim)) {
janelas.mostraErro("Data inicial não é anterior a data final!");
return pedePeriodo();
}

if (data_inicio.before(data_limite)) {
janelas.mostraErro("Data inicial limite é 90 dias atrás!");
return pedePeriodo();
}

if (data_fim.after(hoje)) {
janelas.mostraErro("Data final não pode ser data futura!");
return pedePeriodo();
}

return periodo;
}

function
pedeLocaisEmbarque()
{
_default_rf_embarques = new
java.util.ArrayList();
_default_rf_embarques.add(UNIDADE_RF);
_cod_rf_embarques = janelas.pedeLista("Indique as unidades RF de embarque OU despacho", _default_rf_embarques);

if (_cod_rf_embarques.size() < 1) {
janelas.mostraErro("Deve fornecer pelo menos uma unidade RF!");
return pedeLocaisEmbarque();
}

return _cod_rf_embarques;
}


function
detalhaDUEs(periodo, cod_rf_embarques, acrescenta)
{
web.desabilitaNavegacaoCompleta(true);
if (!loginPucomex()) {
throw "SMARTCARD LOGIN ERROR";
}

var
dues_salvas = [];

for (_icod_rf = 0; _icod_rf < cod_rf_embarques.size(); _icod_rf++) {
cod_rf_embarque = _cod_rf_embarques.get(_icod_rf);

if (cod_rf_embarque == null) {
continue;
}

if (typeof acrescenta == = "undefined") {acrescenta = false;}

_tabela_dues = tabelas.novaTabela("ANR EXP - DUEs",
"DUE", "TEXTO", "CPFCNPJ DECLARANTE", "TEXTO", "NOME DECLARANTE", "TEXTO",
"SITUACAO", "TEXTO", "CANAL", "TEXTO",
"PAIS IMPORTADOR", "TEXTO", "RECINTO DESPACHO", "TEXTO", "RECINTO EMBARQUE", "TEXTO"
);

_tabela_due_itens = tabelas.novaTabela("ANR EXP - DUE ITENS",
"DUE", "TEXTO", "ITEM", "TEXTO",
"CPFCNPJ EXPORTADOR", "TEXTO", "NOME EXPORTADOR", "TEXTO", "UF", "TEXTO",
"NOME IMPORTADOR", "TEXTO", "ENDERECO IMPORTADOR", "TEXTO", "PAIS IMPORTADOR", "TEXTO",
"NCM", "TEXTO", "DESCRICAO NCM", "TEXTO", "DESCRICAO MERCADORIA", "TEXTO",
"PESO LIQ", "DECIMAL",
"CONDICAO DE VENDA", "TEXTO", "MOEDA", "TEXTO",
"VMCV", "DECIMAL", "VMLE", "DECIMAL", "VALOR NFE (R$)", "DECIMAL"
);

_tabela_due_ctns = tabelas.novaTabela("ANR EXP - DUE CONTEINERES",
"DUE", "TEXTO", "CONTEINER", "TEXTO", "LACRE", "TEXTO",
"SITUACAO", "TEXTO",
"TARA", "NUMERO"
);

data_atual = periodo.getDataInicial();

while (data_atual.before(periodo.getDataFinal().somaDias(1))) {
var data_atual_formatada = data_atual.formata("AAAAMMDD");

println("Data: " + data_atual);
println("Listando DUEs com unidade de despacho " + cod_rf_embarque);
var list_url = "https://portalunico.siscomex.gov.br/due/api/due/listar-due-consulta?periodoInicial="+data_atual_formatada+"&periodoFinal="+data_atual_formatada+"&unidadeDespacho="+cod_rf_embarque;
web.addCabecalhoExtra("X-CSRF-Token", this.csrf_token)
if (web.abrirPaginaTrataErro(list_url)) {
dues = JSON.parse(web.getPaginaAtual().getTexto())["listaDueCover"]

for (var idue in dues) {
var due = dues[idue];
dues_salvas.push(due["numero"]);
}
}

println("Listando DUEs com unidade de embarque " + cod_rf_embarque + " e unidade de despacho diversa");
list_url = "https://portalunico.siscomex.gov.br/due/api/due/listar-due-consulta?periodoInicial="+data_atual_formatada+"&periodoFinal="+data_atual_formatada+"&unidadeEmbarque="+cod_rf_embarque;
web.addCabecalhoExtra("X-CSRF-Token", this.csrf_token)
if (web.abrirPaginaTrataErro(list_url)) {
dues = JSON.parse(web.getPaginaAtual().getTexto())["listaDueCover"]

for (var idue in dues) {
var due = dues[idue];
// println(JSON.stringify(due))
if (due["unidadeLocalDespacho"]["codigo"] == due["unidadeLocalEmbarque"]["codigo"]) {
continue;
}

dues_salvas.push(due["numero"]);
}
}

data_atual = data_atual.somaDias(1);
}
}

var
dues_completas = executaParalelo("Biblioteca Pucomex", "populaDadosDUEParalelo", dues_salvas, MAX_PARALELO);

for (var idue in dues_completas)
{
    var
due = dues_completas[idue];
var
due_itens = due["listaInfoItemDue"];

due["paisImportador"] = due["paisImportador"] | | {};
due["recintoAduaneiroDespacho"] = due["recintoAduaneiroDespacho"] | | {};
due["recintoAduaneiroEmbarque"] = due["recintoAduaneiroEmbarque"] | | {};

_tabela_dues.addValores([
    due["numero"], due["niDeclarante"]["numero"], due["niDeclarante"]["nome"],
    due["situacaoDue"], String(due["canalBD"]),
    String(due["paisImportador"]["codigo"]), String(due["recintoAduaneiroDespacho"]["codigo"]),
    String(due["recintoAduaneiroEmbarque"]["codigo"])
]);

for (var _iitem in due_itens)
{
    var
due_item = due_itens[_iitem];

var
nfe;
if (due_item["itemNotaFiscalExportacao"])
{
    nfe = due_item["itemNotaFiscalExportacao"]["notaFiscal"];
} else {
    nfe = {"niEmitente": {"numero": null, "nome": null}, "ufEmissor": null}
}

due_item["ncm"] = due_item["ncm"] | | {};
due_item["condicaoVenda"] = due_item["condicaoVenda"] | | {};
due_item["moeda"] = due_item["moeda"] | | {};

_tabela_due_itens.addValores([
    due["numero"], String(due_item["id"]),
    nfe["niEmitente"]["numero"], nfe["niEmitente"]["nome"], nfe["ufEmissor"],
    due_item["nomeImportador"], due_item["enderecoImportador"], due_item["paisImportador"],
    String(due_item["ncm"]["codigo"]), due_item["ncm"]["descricao"],
    due_item["descricaoMercadoria"] + " > " + due_item["descricaoComplementar"],
    due_item["pesoLiquidoTotal"],
    String(due_item["condicaoVenda"]["codigo"]), String(due_item["moeda"]["codigo"]),
    due_item["valorCondicaoVenda"], due_item["valorLocalEmbarque"], due_item["valorTotal"]
]);
}

var
due_conteineres = due["conteineres"]; // | | [];

for (var ictn in due_conteineres)
{
    var
conteiner = due_conteineres[ictn];

conteiner["lacres"] = conteiner["lacres"] | | [];
conteiner["saldo"] = conteiner["saldo"] | | {};

_tabela_due_ctns.addValores([
    due["numero"],
    conteiner["identificacao"], conteiner["lacres"].join(", "),
    conteiner["saldo"]["descricao"],
    conteiner["tara"]
]);
}
}

_tabela_dues.exportaTabelaUsuario("ANR EXP - DUEs", acrescenta);
_tabela_due_itens.exportaTabelaUsuario("ANR EXP - DUE ITENs", acrescenta);
_tabela_due_ctns.exportaTabelaUsuario("ANR EXP - DUE CONTEINERES", acrescenta);

}

function
detalhaEstoqueDUEs(periodo, cod_urfs, acrescenta)
{
    web.desabilitaNavegacaoCompleta(true);
if (!loginPucomex()) {
    throw
"SMARTCARD LOGIN ERROR";
}

var
tabela_due_estoques = tabelas.novaTabela("ANR EXP - CARGAS ESTOCADAS",
                                         "ID", "TEXTO", "COD URF", "TEXTO", "NOME URF", "TEXTO",
                                         "COD RECINTO", "TEXTO", "NOME RECINTO ", "TEXTO",
                                         "DATA ENTRADA", "DATA", "HORA ENTRADA", "TEXTO",
                                         "CPFCNPJ RESPONSAVEL", "TEXTO", "NOME RESPONSAVEL", "TEXTO",
                                         "PAIS RESPONSAVEL", "TEXTO",
                                         "NUMERO NF", "TEXTO", "MODELO NF", "TEXTO", "SERIE NF", "TEXTO", "EMISSAO NF",
                                         "TEXTO", "UF EMISSAO NF", "TEXTO",
                                         "CPFCNPJ EMITENTE NF", "TEXTO", "NOME EMITENTE NF", "TEXTO",
                                         "PAIS EMITENTE NF", "TEXTO",
                                         "CPFCNPJ DESTINATARIO NF", "TEXTO", "NOME DESTINATARIO NF", "TEXTO",
                                         "PAIS DESTINATARIO NF", "TEXTO",
                                         "CPFCNPJ TRANSPORTADOR", "TEXTO", "NOME TRANSPORTADOR", "TEXTO",
                                         "PAIS TRANSPORTADOR", "TEXTO",
                                         "CPFCNPJ CONDUTOR", "TEXTO", "NOME CONDUTOR", "TEXTO", "PAIS CONDUTOR",
                                         "TEXTO",
                                         "COD NCM", "TEXTO", "DESCRICAO NCM", "TEXTO",
                                         "AVARIAS", "TEXTO",
                                         "PESO", "DECIMAL", "UN ESTATISTICA", "TEXTO", "VALOR", "MOEDA"

                                         );

var
tabela_due_estoques_ctns = tabelas.novaTabela("ANR EXP - CARGAS ESTOCADAS CONTEINERES",
                                              "ID", "TEXTO", "CONTEINER", "TEXTO", "LACRE", "TEXTO",
                                              "SITUACAO", "TEXTO",
                                              "TARA", "NUMERO"
                                              );

var
pre_estoques = [];

for (var icod_urf = 0; icod_urf < cod_urfs.size();
icod_urf + +) {
    var
cod_urf = cod_urfs.get(icod_urf);

var
data_atual = periodo.getDataInicial();

while (data_atual.before(periodo.getDataFinal().somaDias(1))) {
var data_atual_formatada = data_atual.formata("AAAA-MM-DD");

println("Data: " + data_atual);
println("Listando Cargas na unidade RF " + cod_urf);

var list_url = "https://portalunico.siscomex.gov.br/cct/api/deposito-carga/consultar-estoque-antes-acd?codigoURF=" + cod_urf +
"&dataEntradaFinal="+data_atual_formatada+"T04:00:00.000Z&dataEntradaInicial="+data_atual_formatada+"T04:00:00.000Z";

web.addCabecalhoExtra("X-CSRF-Token", this.csrf_token)
if (web.abrirPaginaTrataErro(list_url)) {
var previa_diaria = JSON.parse(web.getPaginaAtual().getTexto())
pre_estoques = pre_estoques.concat(previa_diaria["lista"]);
}

data_atual = data_atual.somaDias(1);
}
}

var
pre_estoques_detalhados = executaParalelo("Biblioteca Pucomex", "estoquePreACDParalelo", pre_estoques, MAX_PARALELO);

for (var iestoque = 0; iestoque < pre_estoques_detalhados.length; iestoque++)
{
var
detalhe_estoque = pre_estoques_detalhados[iestoque];

var
data_entrada, hora_entrada;

if (detalhe_estoque["dataHoraEntradaEstoque"]) {
data_entrada = new Date(detalhe_estoque["dataHoraEntradaEstoque"]);
hora_entrada = ("00" + data_entrada.getHours()).slice(-2) + ":" + ("00" + data_entrada.getMinutes()).slice(-2);
}

detalhe_estoque["local"] = detalhe_estoque["local"] | | {"urf": {}, "ra": {}};
detalhe_estoque["notaFiscal"] = detalhe_estoque["notaFiscal"] | | {"emitente": {}, "destinatario": {}};
detalhe_estoque["responsavel"] = detalhe_estoque["responsavel"] | | {};
detalhe_estoque["condutor"] = detalhe_estoque["condutor"] | | {};
detalhe_estoque["transportador"] = detalhe_estoque["transportador"] | | {};
detalhe_estoque["ncm"] = detalhe_estoque["ncm"] | | {};

var
nf = detalhe_estoque["notaFiscal"];

tabela_due_estoques.addValores([
    detalhe_estoque["id"], detalhe_estoque["local"]["urf"]["codigo"], detalhe_estoque["local"]["urf"]["descricao"],
    detalhe_estoque["local"]["ra"]["codigo"], detalhe_estoque["local"]["ra"]["descricao"],
    data_entrada, hora_entrada,
    detalhe_estoque["responsavel"]["identificacao"], detalhe_estoque["responsavel"]["nome"],
    detalhe_estoque["responsavel"]["pais"],
    nf["numero"], nf["modelo"], nf["serie"], nf["emissao"], nf["uf"],
    nf["emitente"]["identificacao"], nf["emitente"]["nome"], nf["emitente"]["pais"],
    nf["destinatario"]["identificacao"], nf["destinatario"]["nome"], nf["destinatario"]["pais"],
    detalhe_estoque["transportador"]["identificacao"], detalhe_estoque["transportador"]["nome"],
    detalhe_estoque["transportador"]["pais"],
    detalhe_estoque["condutor"]["identificacao"], detalhe_estoque["condutor"]["nome"],
    detalhe_estoque["condutor"]["pais"],
    detalhe_estoque["ncm"]["codigo"], detalhe_estoque["ncm"]["descricao"],
    detalhe_estoque["descricaoAvarias"],
    detalhe_estoque["pesoAferido"], detalhe_estoque["unidadeEstatistica"], detalhe_estoque["valor"],
]);

var
conteineres = detalhe_estoque["conteineres"]; // | | [];

for (var ictn in conteineres) {
    var conteiner = conteineres[ictn];

conteiner["lacres"] = conteiner["lacres"] | |[];
conteiner["saldo"] = conteiner["saldo"] | | {};

tabela_due_estoques_ctns.addValores([
detalhe_estoque["id"],
conteiner["identificacao"], conteiner["lacres"].join(", "),
conteiner["saldo"]["descricao"],
conteiner["tara"]
]);
}
}

tabela_due_estoques.exportaTabelaUsuario("ANR EXP - CARGAS ESTOCADAS", acrescenta);
tabela_due_estoques_ctns.exportaTabelaUsuario("ANR EXP - CARGAS ESTOCADAS CONTEINERES", acrescenta);
}

function
escape_control_chars(text)
{
    text = String(text);
text = text.replace( / ([ ^\\])\n / g, '$1\\n');
return text;
}

function
criaTabelasIniciais()
{
_tabela_ddes = tabelas.novaTabela("ANR EXP - DDEs",
                                  "DDE", "TEXTO", "SITUACAO", "TEXTO", "DATA SITUACAO", "TEXTO",
                                  "CNPJ", "CNPJ14", "EMPRESA", "TEXTO", "DESPACHANTE", "TEXTO",
                                  "VOLUMES", "NÚMERO", "PESO BRUTO TOTAL", "DECIMAL", "VALOR US$", "DECIMAL",
                                  "A POSTERIORI", "TEXTO", "REs", "NÚMERO",
                                  "UA DESPACHO", "TEXTO", "UA EMBARQUE", "TEXTO", "RECINTO", "TEXTO"
                                  );

_tabela_res = tabelas.novaTabela("ANR EXP - REs",
                                 "DDE", "TEXTO", "RE", "TEXTO", "VALOR US$", "DECIMAL", "INCOTERM", "TEXTO",
                                 "PESO LIQ", "DECIMAL", "DESTINO", "TEXTO", "NCM", "TEXTO", "Desc NCM", "TEXTO"
                                 );

_tabela_conteineres = tabelas.novaTabela("ANR EXP - Conteineres",
                                         "DDE", "TEXTO", "CONTEINER", "TEXTO", "LACRE", "TEXTO"
                                         );

return {
    "tabela_ddes": _tabela_ddes,
    "tabela_res": _tabela_res,
    "tabela_conteineres": _tabela_conteineres
}
}

function
carregaPresencaCarga_web()
{
var
conteineres = [];
var
tabs = web.getPaginaAtual().getTabelas()
for (indexTab=0; indexTab < tabs.size(); indexTab++) {
tab = tabs.get(indexTab)
if (tab != null & & tab.getCelula(0, 0) == 'Conteiner') {
for (linha=1; linha < tab.getNumLinhas(); linha++) {
if (tab.getCelula(linha, 0) != null) {
conteineres.push({
"DDE": _dde,
       "conteiner": tab.getCelula(linha, 0),
"lacre": tab.getCelula(linha, 1)
});
}
}
}
}

return conteineres;
}

/ *
*
*UTILITÁRIOS
 *
 * /

 function
existeTabela(_nome_tabela)
{
_tabela = contagil.getTabelaUsuario(_nome_tabela);
if (!_tabela) {
janelas.mostraErro("Tabela do Usuário não encontrada: " + _nome_tabela);
throw "TabelaUsuario not found";
}
}


function
loginPucomex()
{
web.setAutoRedirecionamento(true)
web.setUseSmartcard(true)

web.abrirPagina("https://www.suiterfb.receita.fazenda")
var form = new WebFormulario()
form.setMetodoPOST();
form.setURL("https://portalunico.siscomex.gov.br/portal/proxy/authenticate");
web.addCabecalhoExtra("Credential-Type", "USUARIO_PORTAL")
web.addCabecalhoExtra("Role-Type", "rfb")
web.submeterFormulario(form);

executor.aguardarTempo(500)

this.csrf_token = web.getCabecalhoValor("X-CSRF-Token")

if (this.csrf_token == null) {
throw "ERRO DE LOGIN: PUCOMEX";
}
return true
}

function
pegaValorAbaixo(_atributo, _linhas)
{
for (_ilinha in _linhas) {
    _linha = _linhas[_ilinha];
if (_linha.match(_atributo)) {
return _linhas[parseInt(_ilinha) + 1].trim();
}
}
return ""
}

function
unique(array)
{
if (array == null) {
return;
}
return array.filter(function(el, i, a)
{ if (i == a.indexOf(el))
return 1;
return 0})
}

function
replaceAll(str, de, para)
{
    var
str = String(str);
var
pos = str.search(de);
while (pos > -1){
str = str.replace(de, para);
pos = str.search(de);
}
return (str);
}

function
printObj(object)
{
var
output = '';
for (var property in object) {
    output += property + ': ' + object[property]+'; \n';
}
println(output)
}


PROGRESS_CONTEXT = {};
function
showProgress(context, current, total)
{

if (typeof(PROGRESS_CONTEXT) == "undefined") {
PROGRESS_CONTEXT = {};
}
if (!PROGRESS_CONTEXT[context] | | current == 0) {
var start_time = new Date().getTime();
PROGRESS_CONTEXT[context] = {
"start_time": start_time
}
}

var
average_time = "NaN";
if (current > 0) {
average_time = (new Date().getTime() - PROGRESS_CONTEXT[context].start_time) / (current + 1)
}
var
remaining = Math.round(((total - current - 1) * average_time / 1000 / 60) * 100) / 100
var
progress = Math.round(100 * 100 * current / (total - 1)) / 100;

println("Progresso: " + progress + "%");
println("Tempo restante estimado: " + remaining + " minuto(s)");
}

//  ######################################################

// NOTE: QUANDO
EXECUTA
RUNSCRIPT
ELE
PERDE
AS
EXCECOES
JOGADAS, UM
FIX
SERIA
ESSE
// function
aguardaTelaHod(_texto)
{
// if (!contagil.runScript(BIBLIOTECA, "aguardaTelaHod", _texto)) {throw "TELA ERRADA DO HOD";}
//}

function
executaParalelo(script, metodo, params, max_paralelo)
{
var
exec_paralelas = []
var
retornos = []

showProgress("EXECUCAO", 0, params.length);
for (var iparam = 0; iparam < params.length; iparam++) {
    var param = params[iparam];

if (DEBUG) {
println("Executando paralelamente '" + script + "'." + metodo + "(" + param + ")");
}

if (exec_paralelas.length < max_paralelo) {
var exec_paralela = contagil.runScriptParalelamente(script, metodo, param)
exec_paralelas.push(exec_paralela)
}

if (exec_paralelas.length == max_paralelo | | iparam == params.length - 1) {

for (var iexec in exec_paralelas) {
var exec_paralela = exec_paralelas[iexec];
exec_paralela.aguarda();

if (exec_paralela.hasErro()) {
executor.mostraErro(exec_paralela.getUltimaMensagemErro());
} else {
if (exec_paralela.getRetorno()) {
retornos.push(exec_paralela.getRetorno());
}
}
}

exec_paralelas =[]
showProgress("EXECUCAO", iparam, params.length);
}

}

return retornos
}

function
executaSequencial(script, metodo, params)
{
var
retornos = [];
retornos.push(contagil.runScript(script, metodo, param));

return retornos;
}
"""