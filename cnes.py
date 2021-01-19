
###  CNES

# Versão Python: 112020

## Cadastro Nacional de Estabelecimentos de Saúde

#Faz as importações dos pacotes
import pandas as pd
import numpy as np
import os
import ftplib
import zipfile

#Define o mês e ano dos dados
data_cnes = ('202009')
filename = ('BASE_DE_DADOS_CNES_'+data_cnes+'.ZIP')

# Faz o download automático do FTP do Datasus
ftp = ftplib.FTP('ftp.datasus.gov.br')
ftp.login('', '')
ftp.cwd ('cnes/')
ftp.retrbinary('RETR ' + filename, open(filename, 'wb').write)
ftp.quit()

#Faz a descompactação do arquivo zip no diretório do Google
with zipfile.ZipFile(filename,'r') as zip_ref:
     zip_ref.extractall('/content')

#Faz as leituras das bases de dados
cnes_ch = pd.read_csv('/content/tbCargaHorariaSus'+data_cnes+'.csv', 
                      usecols=[0,1,2,3,4,6,12,13], sep=';',header=0,
                      names=['num_unidade', 'num_profissional','cod_cbo','ind_sus','cod_vinculo_total', 'qtd_ch_ambulatorial',
                             'num_cnpj_empregador','data_atualiza_vinculo'],
                      dtype={'num_unidade':str, 'num_profissional':str,'cod_cbo':str,'ind_sus':str,'num_cnpj_empregador':str,'cod_vinculo_total':int},
                      parse_dates=['data_atualiza_vinculo'], encoding='latin-1')

cnes_prof = pd.read_csv('/content/tbDadosProfissionalSus'+data_cnes+'.csv',
                        usecols=[0,2,3], sep=';', header=0,
                        names=['num_profissional','nom_profissional','num_cns_profissional'],
                        dtype={'num_profissional':str, 'nom_profissional':str, 'num_cns_profissional':str},
                        encoding='latin-1')

cnes_estab = pd.read_csv('/content/tbEstabelecimento'+data_cnes+'.csv', sep=';', header=0,
                         usecols=(0,1,2,3,4,5,6,19,20,21,28,29,31,43,49), names=['num_unidade','num_cnes','num_cnpj_mantenedora',
                        'cod_pessoa','cod_situacao','nom_razao_social','nom_fantasia','num_cpf_estabelecimento',
                        'num_cnpj_estabelecimento','cod_atividade','cod_tipo','cod_turno','cod_mun','cod_natureza','cod_gestao'],
                         dtype={'num_unidade':str,'num_cnes':str,'num_cnpj_mantenedora':str,'cod_pessoa':int,'cod_situacao':int,
                        'nom_razao_social':str,'nom_fantasia':str,'num_cpf_estabelecimento':str,'num_cnpj_estabelecimento':str,
                        'cod_atividade':int,'cod_tipo':int,'cod_turno':float,'cod_mun':int,'cod_natureza':float,'cod_gestao':str})

cnes_estab_sub = pd.read_csv('/content/rlEstabSubTipo'+data_cnes+'.csv', sep=';',
                            header=0, usecols=(0,2), names=['num_unidade', 'cod_subtipo'], 
                            dtype={'num_unidade':str, 'cod_subtipo':int})

cnes_estab_mant = pd.read_csv('/content/tbMantenedora'+data_cnes+'.csv', sep=';',
                             encoding='latin-1', usecols=(0,16,20), header=0,
                             names=['num_cnpj_mantenedora', 'cod_natureza_mantenedora', 'cod_ibge_mantenedora'],
                             dtype={'num_cnpj_mantenedora':str, 'cod_natureza_mantenedora':float, 'cod_ibge_mantenedora':str})

cnes_estab_nivel = pd.read_csv('/content/rlEstabProgFundo'+data_cnes+'.csv',
                              sep=';', header=0, usecols=(0,1), names=['num_unidade', 'cod_nivel'], 
                              dtype={'num_unidade':str, 'cod_nivel':int}, encoding='latin-1')

cnes_estab_tipo_atend = pd.read_csv('/content/rlEstabAtendPrestConv'+data_cnes+'.csv',
                              sep=';', header=0, usecols=(0,1), names=['num_unidade', 'cod_atprest'], 
                              dtype={'num_unidade':str, 'cod_atprest':int}, encoding='latin-1')

info_geo = pd.read_csv('/content/regioes_imediatas_intermediarias_ibge.txt', sep='\t', 
                       header=0, dtype={'cod_mun_7':int,'cod_mun':int,'nome_mun':str,'cod_rgi':int,'nome_rgi':str,'cod_rgint':int,'nome_rgint':str},
                       encoding='latin-1')

"""### Parte 1 - Carga Horária"""

# Retira os vínculos sem Carga Horária Ambulatorial
# Apaga os registros sem num_unidade e num_profissional
cnes_ch.dropna(subset=['qtd_ch_ambulatorial','num_unidade','num_profissional'],how='any', inplace=True)

# Exclui duplicados
cnes_prof.drop_duplicates('num_profissional', keep='first', inplace=True)

# Agrega os bancos
cnes_ch = pd.merge(cnes_ch, cnes_prof,on='num_profissional', how='inner')

# Altera a Variável ind_sus
cnes_ch['ind_sus'] = cnes_ch.ind_sus.replace({'N':0, 'S':1, 's':1})

# Coloca a primeira letra do nome do profissional em letra maiuscula

# Coloca a variável de Carga Horária Ambulatorial como Integral
cnes_ch['qtd_ch_ambulatorial'] = cnes_ch['qtd_ch_ambulatorial'].astype('int64')

#Cria Variável de Carga Horária Total
cnes_ch['qtd_ch_total'] = cnes_ch['qtd_ch_ambulatorial']

# Cria as categorias da Variável Carga Horária Total
cnes_ch['cod_ch_total'] = np.select ([
    (cnes_ch['qtd_ch_total'] >= 1) & (cnes_ch['qtd_ch_total'] <= 19),
    (cnes_ch['qtd_ch_total'] == 20),
    (cnes_ch['qtd_ch_total'] >= 21) & (cnes_ch['qtd_ch_total'] <= 29),
    (cnes_ch['qtd_ch_total'] == 30),
    (cnes_ch['qtd_ch_total'] >= 31) & (cnes_ch['qtd_ch_total'] <= 39),
    (cnes_ch['qtd_ch_total'] == 40),
    (cnes_ch['qtd_ch_total'] >= 41) & (cnes_ch['qtd_ch_total'] <= 80),
    (cnes_ch['qtd_ch_total'] > 80)], [1,2,3,4,5,6,7,8], default=99)

# cod_vinculacao : "Tipo de vinculação do profissional com o estabelecimento"
cnes_ch['cod_vinculacao'] = cnes_ch.cod_vinculo_total.replace({
    0:9,10000:1,10100:1,10101:1,10102:1,10200:1,10201:1,10202:1,
    10203:1,10300:1,10301:1,10302:1,10400:1,10401:1,10402:1,10403:1,
    10404:1,10405:1,10500:1,10501:1,10502:1,10503:1,10504:1,20000:2,
    20100:6,20200:6,20300:6,20400:6,20500:6,20600:2,20700:2,20800:6,
    20900:2,21000:2,21100:2,30000:2,40100:5,40200:7,40300:2,50000:4,
    50101:4,50102:4,60000:3,60101:3,60102:3,70100:5,70101:5,70102:5,
    80000:6,80100:6,80200:6,80300:6,80400:6,80501:6,80502:6,80600:6,
    90100:7,90200:7,100100:7,100200:7,100300:7})

# Cria a variável do Código do Vínculo
cnes_ch['cod_vinculo'] = cnes_ch.cod_vinculo_total.replace({
    0:99,10000:0,10100:1,10101:1, 10102:1, 10200:2,10201:2,10202:2,10203:2,10300:3,10301:3,10302:3,10400:4, 
    10401:4,10402:4,10403:4,10404:4,10405:4,10500:5,10501:5,10502:5,10503:5,10504:5,20000:6,20100:10,20200:11,20300:12, 
    20400:13,20500:14,20600:7,20700:8,20800:9,20900:6,21000:6,21100:9,30000:9,40100:15,40200:18,40300:6,50000:16,
    50101:16,50102:16,60000:17,60101:17,60102:17,70100:15,70101:15,70102:15,80000:14,80100:2,80200:3,80300:4, 
    80400:5,80501:6,80502:6,80600:9,90100:18,90200:18,100100:19,100200:19,100300:19})
cnes_ch.fillna({'cod_vinculo':99},inplace=True)

# Cria a variável do Código do Subvínculo - Olhar questão do (else=98)
cnes_ch['cod_subvinculo'] = cnes_ch.cod_vinculo_total.replace({
    10201:3,10300:98,10401:2,10402:1,10501:6,10502:5,10503:4,10504:7,10101:8,10102:9,10202:10,10203:11,10301:12,
    10302:13,10403:14,10404:15,10405:16,50101:17,50102:18,60101:19,60102:20,70101:21,70102:22,80100:23,80200:24,
    80300:25,80400:26,80501:27, 80502:28,80600:29,90100:30,90200:31,100100:32,100200:33,100300:34})
cnes_ch.fillna({'cod_subvinculo':99},inplace=True)

# Cria agregações de tipo de vínculo
cnes_ch['cod_vinculo_agregado'] = cnes_ch.cod_vinculo_total.replace({
    0:99,10000:13,10100:1,10101:1,10102:1,10200:2,10201:2,10202:2,10203:2,10300:5,10301:5,10302:5,
    10400:3,10401:3,10402:3,10403:3,10404:3,10405:3,10500:4,10501:4,10502:4,10503:4,10504:4,20000:14,
    20100:14,20200:14,20300:14,20400:14,20500:14,20600:6,20700:6,20800:8,20900:7,21000:6,21100:8,30000:8,
    40100:9,40200:12,40300:7,50000:10,50101:10,50102:10,60000:11,60101:11,60102:11,70100:9,70101:9,70102:9,
    80000:14,80100:2,80200:5,80300:3,80400:4,80501:7,80502:6,80600:8,90100:12,90200:12,100100:1,100200:2,100300:3})
cnes_ch.fillna({'cod_vinculo_agregado':99},inplace=True)

#Altera o tipo da variável cod_vinculo_agregado para integral
cnes_ch['cod_vinculo_agregado'] = cnes_ch['cod_vinculo_agregado'].astype('int64')

# Cria Variável de Indicador de trabalho protegido
cnes_ch['ind_protegido'] = np.select ([
    (cnes_ch['cod_vinculo_agregado'] >= 1) & (cnes_ch['cod_vinculo_agregado'] <= 4),
    (cnes_ch['cod_vinculo_agregado'] >= 5) & (cnes_ch['cod_vinculo_agregado'] <= 12),
    (cnes_ch['cod_vinculo_agregado'] >= 13) & (cnes_ch['cod_vinculo_agregado'] <= 14),
    (cnes_ch['cod_vinculo_agregado'] == 99)], [1,2,3,9])

# Define as Labels
cnes_ch['ind_sus_label'] = cnes_ch.ind_sus.map({0:'Vínculo não SUS', 1:'Vínculo SUS', 1:'Vínculo SUS'})

cnes_ch['cod_ch_total_label'] = cnes_ch.cod_ch_total.map({
1:'1 a 19 horas',
2:'20 horas',
3:'21 a 29 horas',
4:'30 horas',
5:'31 a 39 horas',
6:'40 horas',
7:'Mais de 40 horas',
8:'Sem informação ou inconsistente'})

cnes_ch['cod_vinculo_total_label'] = cnes_ch.cod_vinculo_total.map({
0:'Não informado',
10000:'Vínculo empregatício sem tipo',
10100:'Estatutário sem subtipo (antigo)',
10101:'Estatutário efetivo, servidor próprio',
10102:'Estatutário efetivo, servidor cedido',
10200:'Empregado público celetista sem subtipo',
10201:'Emprego público, subtipo CLT',
10202:'Empregado público celetista próprio',
10203:'Empregado público celetista cedido',
10300:'Contrato por prazo determinado, sem subtipo',
10301:'Contrato por prazo determinado, público',
10302:'Contrato por prazo determinado, privado',
10400:'Cargo comissionado, sem subtipo',
10401:'Cargo comissionado, subtipo cargo comissionado não cedido',
10402:'Cargo comissionado, subtipo cargo comissionado cedido',
10403:'Cargo comissionado, servidor público próprio',
10404:'Cargo comissionado, servidor público cedido',
10405:'Cargo comissionado, sem vínculo com o setor público',
10500:'Celetista',
10501:'Celetista, subtipo contrato por OSCIP/OS',
10502:'Celetista, subtipo contrato por ONG',
10503:'Celetista, subtipo contrato por entidade filantrópica',
10504:'Celetista, subtipo contrato por rede privada',
20000:'Autônomo sem tipo',
20100:'Intermediado por OS',
20200:'Intermediado por OSCIP',
20300:'Intermediado por ONG',
20400:'Intermediado por entidade filantrópica',
20500:'Intermediado por empresa privada',
20600:'Consultoria',
20700:'RPA',
20800:'Intermediado por cooperativa',
20900:'Autônomo, Pessoa Jurídica',
21000:'Autônomo, Pessoa Física',
21100:'Autônomo, cooperado',
30000:'Cooperado',
40100:'Bolsa',
40200:'Contrato verbal/informal',
40300:'Proprietário',
50000:'Residente, sem subtipo',
50101:'Residente, próprio',
50102:'Residente, subsidiado por outro ente/entidade',
60000:'Estagiário, sem subtipo',
60101:'Estagiário, próprio',
60102:'Estagiário, subsidiado por outro ente/entidade',
70100:'Bolsista, Sem subtipo',
70101:'Bolsista, próprio',
70102:'Bolsista, subsidiado por outro ente/entidade',
80000:'Intermediado, sem subtipo',
80100:'Intermediado, empregado público celetista',
80200:'Intermediado, contrato temporário ou por prazo',
80300:'Intermediado, cargo comissionado',
80400:'Intermediado, celetista',
80501:'Intermediado, autônomo Pessoa Jurídica',
80502:'Intermediado, autônomo Pessoa Física',
80600:'Intermediado, cooperado',
90100:'Informal, contratado verbalmente',
90200:'Informal, voluntariado',
100100:'Servidor público cedido para iniciativa privada',
100200:'Empregado público celetista cedido para iniciativa privada',
100300:'Comissionado cedido para iniciativa privada'})

cnes_ch['cod_vinculacao_label'] = cnes_ch.cod_vinculacao.map({
1:'Vínculo empregatício',
2:'Autônomo',
3:'Estágio',
4:'Residência',
5:'Bolsa',
6:'Intermediado',
7:'Outros',
9:'Não informado'})

cnes_ch['cod_vinculo_label'] = cnes_ch.cod_vinculo.map({
0:'Vínculo empregatício sem tipo',
1:'Estatutário',
2:'Empregado público',
3:'Contrato por prazo determinado',
4:'Cargo comissionado',
5:'Celetista',
6:'Autônomo',
7:'Consultoria',
8:'RPA',
9:'Cooperado',
10:'Intermediado por OS (antigo)',
11:'Intermediado por OSCIP (antigo)',
12:'Intermediado por ONG (antigo)',
13:'Intermediado por entidade filantrópica:(antigo)',
14:'Intermediado por empresa privada:(antigo)',
15:'Bolsa',
16:'Residente',
17:'Estagiário',
18:'Contrato informal',
19:'Servidor público cedido para iniciativa privada',
99:'Não informado'})

cnes_ch['cod_subvinculo_label'] = cnes_ch.cod_subvinculo.map({
1:'Cargo Comissionado Cedido (antigo)',
2:'Cargo Comissionado Não Cedido (antigo)',
3:'Emprego público celetista (antigo)',
4:'Contrato por entidade filantrópica',
5:'Contrato por ONG',
6:'Contrato por OSCIP/OS',
7:'Contrato por rede privada',
8:'Estatutário efetivo, servidor próprio',
9:'Estatutário efetivo, servidor cedido',
10:'Empregado Público celetista próprio',
11:'Empregado Público celetista cedido',
12:'Contrato Temporário com a Administração Pública',
13:'Contrato por prazo determinado no setor privado',
14:'Cargo Comissionado, servidor público próprio',
15:'Cargo Comissionado, servidor público cedido',
16:'Cargo Comissionado, sem vínculo com o setor público',
17:'Residente próprio',
18:'Residente subsidiado por outro ente/entidade',
19:'Estagiário próprio',
20:'Estagiário subsidiado por outro ente/entidade',
21:'Bolsista próprio',
22:'Bolsista subsidiado por outro ente/entidade',
23:'Intermediado, empregado público celetista',
24:'Intermediado, contrato temporário',
25:'Intermediado, cargo comissionado:',
26:'Intermediado, celetista',
27:'Intermediado, autônomo pessoa jurídica',
28:'Intermediado, autônomo pessoa física',
29:'Intermediado, cooperado',
30:'Informal, contratado verbalmente',
31:'Informal, voluntariado',
32:'Servidor público cedido para iniciativa privada',
33:'Empregado público celetista cedido para iniciativa privada',
34:'Comissionado cedido para iniciativa privada',
98:'Sem subtipo',
99:'Não informado'})

cnes_ch['cod_vinculo_agregado_label'] = cnes_ch.cod_vinculo_agregado.map({
1:'Estatutário',
2:'Empregado público',
3:'Comissionado',
4:'Celetista',
5:'Temporário',
6:'Autônomo, pessoa física',
7:'Autônomo, pessoa jurídica',
8:'Cooperado',
9:'Bolsista',
10:'Residente',
11:'Estagiário',
12:'Informal',
13:'Outros, vínculo empregatício sem descrição',
14:'Outros, autônomo sem descrição',
99:'Sem informação'})

cnes_ch['ind_protegido_label'] = cnes_ch.ind_protegido.map({
1:'Protegido',
2:'Desprotegido',
3:'Sem definição',
9:'Não informado'})

"""### Parte 2 - Estabelecimento"""

# Exclui os casos de Missing  
cnes_estab.num_unidade.dropna(inplace=True)
cnes_estab_sub.num_unidade.dropna(inplace=True)
cnes_estab_nivel.num_unidade.dropna(inplace=True)
cnes_estab_tipo_atend.num_unidade.dropna(inplace=True)

# Faz o merge dos bancos complementares
cnes_estab = pd.merge(cnes_estab, cnes_estab_sub, on='num_unidade', how='inner')
cnes_estab = pd.merge(cnes_estab, cnes_estab_mant, on='num_cnpj_mantenedora', how='inner')
cnes_estab = pd.merge(cnes_estab, cnes_estab_nivel, on='num_unidade', how='inner')
cnes_estab = pd.merge(cnes_estab, cnes_estab_tipo_atend, on='num_unidade', how='inner')
cnes_estab = pd.merge(cnes_estab, info_geo, on='cod_mun', how='inner')

# Altera os códigos das cidades satélites de Brasilia
cnes_estab.replace((cnes_estab['cod_mun'] >= 530010) & (cnes_estab['cod_mun'] <= 539999), 5300010, inplace=True)

# Altera a variável Cod_pessoa
cnes_estab.cod_pessoa.replace([{1:3, 3:2}], inplace=True)

# Altera a variável cod_situacao
cnes_estab.cod_situacao.replace([{1:1,3:2}], inplace=True)

# Cria a Variavel Cod Tipo Agregado - Tipo de estabelecimento agregado
cnes_estab['cod_tipo_agregado'] = cnes_estab.cod_tipo.replace({
1:1,2:1,4:4,5:2,7:2,15:1,20:3,21:3,22:5,32:8,36:4,39:6,40:8,42:3,43:8,50:7,60:8,61:8,62:2,64:7,67:7,
68:7,69:6,70:4,71:4,72:8,73:3,74:8,75:8,76:7,77:8,78:8,79:8,80:7,81:7,82:7,83:8})

cnes_estab.cod_atividade.replace([{1:1, 2:2, 3:3, 4:5, 5:4}], inplace=True)
cnes_estab.fillna({'cod_atividade':5}, inplace=True)

# Define o missing do Código Turno
cnes_estab.fillna({'cod_turno':'8'}, inplace=True)

# Natureza da organização agregada
cnes_estab['cod_natureza_agregada'] = cnes_estab.cod_natureza.replace({
1000:4,2000:5,3000:6,1015:1,1023:2,1031:3,1040:1,1058:2,1066:3,1074:1,1082:2,1104:1,1112:2,1120:3,1139:1,1147:2,1155:3,
1163:1,1171:2,1180:3,1198:4,1201:4,1210:4,1228:4,1236:2,1244:3,1252:1,1260:2,1279:3,2011:4,2038:4,2046:5,2054:5,2062:5,
2070:5,2089:5,2097:5,2127:5,2135:5,2143:6,2151:5,2160:5,2178:6,2194:5,2216:5,2224:5,2232:5,2240:5,2259:5,2267:5,2275:5,
2283:5,2291:5,2305:5,2313:5,2321:5,2330:6,3034:6,3069:6,3077:6,3085:6,3107:6,3115:6,3131:6,3204:6,3212:6,3220:6,3239:6,
3247:6,3255:6,3263:6,3271:6,3280:6,3298:6,3301:6,3306:6,3310:6,3999:6,4000:7,4014:7,4022:7,4081:7,4090:7,4111:7,4124:7,
5010:8,5029:8,5037:8})

# Dummy de público versus privado
cnes_estab['ind_natureza'] = np.select([
(cnes_estab['cod_natureza_agregada'] >= 1) & (cnes_estab['cod_natureza_agregada'] <= 4),
(cnes_estab['cod_natureza_agregada'] >= 5) & (cnes_estab['cod_natureza_agregada'] <= 7),
(cnes_estab['cod_natureza_agregada'] == 8)], [1,2,1])

# Substitui para códigos a variável cod_gestao
cnes_estab['cod_gestao'] = cnes_estab.cod_gestao.replace({'E':1, 'M':2, 'D':3, 'S':4})
cnes_estab.fillna({'cod_gestao':4}, inplace=True)

# Transforma a variável cod_subtipo
cnes_estab['cod_subtipo'] = np.select([
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 7) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 36) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 39) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 40) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 40) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 50) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 67) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 68) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 69) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 70) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 72) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 72) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 72) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 72) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 72) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 73) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 73) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 73) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 75) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 75) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 76) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 76) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 76) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 80) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 80) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 80) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 80) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 81) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 82) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 82) & (cnes_estab['cod_subtipo'] == 1),
(cnes_estab['cod_tipo'] == 82) & (cnes_estab['cod_subtipo'] == 1)],
[1,2,3,4,5,6,7,7,7,7,8,9,10,11,12,13,14,15,16,17,17,17,17,17,18,18,
 18,18,18,19,19,19,19,19,20,20,20,20,20,21,21,21,21,21,22,23,24,25,
 26,27,28,28,28,29,30,31,32,33,34,35,36,37,37,37,38,39,40,41,42,43,
 44,45,46,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62])

# Remove os Missings da Variável de Subtipo do Estabelecimento 
cnes_estab.fillna({'cod_subtipo':0}, inplace=True)

# Define o missing do cod_natureza_mantenedora
cnes_estab.fillna({'cod_natureza_mantenedora':9999}, inplace=True)

# Cria dummies por nível de atenção
cnes_estab['ind_atencao_basica'] = np.select([(cnes_estab['cod_nivel'] == 1)],[1])
cnes_estab['ind_media_complexidade'] = np.select([(cnes_estab['cod_nivel'] == 2)],[1])
cnes_estab['ind_alta_complexidade'] = np.select([(cnes_estab['cod_nivel'] == 4)],[1])
cnes_estab['ind_media_complexidade'] = np.select([(cnes_estab['cod_nivel'] == 5)],[1])
cnes_estab['ind_alta_complexidade'] = np.select([(cnes_estab['cod_nivel'] == 6)],[1])

cnes_estab.fillna({'ind_atencao_basica':0}, inplace=True)
cnes_estab.fillna({'ind_media_complexidade':0}, inplace=True)
cnes_estab.fillna({'ind_alta_complexidade':0}, inplace=True)

# Agrega as dummies por unidade - Voltar e ver se é necessário

# Cria dummies por nível de atenção
cnes_estab['ind_internacao'] = np.select([(cnes_estab['cod_atprest'] == 1)],[1])
cnes_estab['ind_ambulatorio'] = np.select([(cnes_estab['cod_atprest'] == 2)],[1])  
cnes_estab['ind_sadt'] = np.select([(cnes_estab['cod_atprest'] == 3)],[1])  
cnes_estab['ind_urgencia'] = np.select([(cnes_estab['cod_atprest'] == 4)],[1])  
cnes_estab['ind_vig_reg'] = np.select([(cnes_estab['cod_atprest'] == 5)],[1])  
cnes_estab['ind_vig_reg'] = np.select([(cnes_estab['cod_atprest'] == 6)],[1])
cnes_estab['ind_vig_reg'] = np.select([(cnes_estab['cod_atprest'] == 7)],[1])

cnes_estab.fillna({'ind_internacao':0}, inplace=True)
cnes_estab.fillna({'ind_ambulatorio':0}, inplace=True)
cnes_estab.fillna({'ind_sadt':0}, inplace=True)
cnes_estab.fillna({'ind_urgencia':0}, inplace=True)
cnes_estab.fillna({'ind_vig_reg':0}, inplace=True)

# Agrega as dummies por unidade - Voltar e ver se é necessário

cnes_estab['cod_pessoa_label'] = cnes_estab.cod_pessoa.map({
1:'Pessoa Fisica',
3:'Pessoa Juridica'})

cnes_estab['cod_situacao_label'] = cnes_estab.cod_situacao.map({
1:'Individual',
3:'Mantido'})

cnes_estab['cod_tipo_label'] = cnes_estab.cod_tipo.map({
1:'Posto de Saúde', 
2:'Centro de Saúde/Unidade Básica de Saúde',
4:'Policlínica',
5:'Hospital Geral',
7:'Hospital Especializado',
15:'Unidade Mista',
20:'Pronto Socorro Geral',
21:'Pronto Socorro Especializado',
22:'Consultório Isolado',
32:'Unidade Móvel Fluvial',
36:'Clínica especializada/Ambulatório de especialidade',
39:'Unidade de Apoio à Diagnose e Terapia (SADT isolado)',
40:'Unidade Móvel Terrestre',
42:'Unidade móvel de nível pré-hospitalar - Urgência/Emergência',
43:'Farmácia',
50:'Unidade de Vigilância em Saúde',
60:'Cooperativa',
61:'Centro de Parto Normal - Isolado',
62:'Hospital-dia (isolado)',
64:'Central de regulação de serviços de saúde',
67:'Laboratório Central de Saúde Pública LACEN',
68:'Secretaria de Saúde',
69:'Centro de Atenção Hemoterapia e ou Hematologia',
70:'Centro de Atenção Psicossocial',
71:'Centro de Apoio à Saúde da Família',
72:'Unidade de Atenção à Saúde Indígena',
73:'Pronto Antedimento',
74:'Polo Academia da Saude',
75:'Telessaude',
76:'Central da regulação medica das urgencias',
77:'Serviço de Atenção Domiciliar Isolado (Home Care)',
78:'Unidade de Atenção em Regime Residencial',
79:'Oficina ortopédica',
80:'Laboratório de Saúde Pública',
81:'Central de regulação',
82:'Central de notificação, captação e distribuição de órgãos estadual',
83:'Polo de prevenção de doenças e agravos e promoção da saúde'})

cnes_estab['cod_tipo_agregado_label'] = cnes_estab.cod_tipo_agregado.map({
1:'Unidade Básica de Saúde',
2:'Hospital',
3:'Pronto Atendimento',
4:'Unidade especializada',
5:'Consultório isolado',
6:'Serviço de apoio diagnóstico e terapêutico',
7:'Unidade não assistencial (gestão, regulação, vigilância, saúde pública)',
8:'Outra unidade'})

cnes_estab['cod_atividade_label'] = cnes_estab.cod_atividade.map({
1:'Unidade universitária',
2:'Unidade escola superior isolada',
3:'Unidade auxiliar de ensino',
4:'Hospital ensino',
5:'Unidade sem atividade de ensino'})

cnes_estab['cod_turno_label'] = cnes_estab.cod_turno.map({
1:'Somente pela manhã',
2:'Somente à tarde',
3:'Manhã e tarde',
4:'Manhã, tarde e noite',
5:'Turnos intermitentes',
6:'Contínuo 24 horas',
7:'Somente à noite',
8:'Não informado'})

# A partir de 12/2015 sofre alterações em relação aos períodos anteriores do CNES. 
# Incorpora a Resolução Concla nº 1, de 28 de abril de 2016
cnes_estab['cod_natureza_label'] = cnes_estab.cod_natureza.map({
1000:'Administração Pública Sem Definição',
1015:'Órgão Público do Poder Executivo Federal',
1023:'Órgão Público do Poder Executivo dos Estados ou do Distrito Federal',
1031:'Órgão Público do Poder Executivo Municipal',
1040:'Órgão Público do Poder Legislativo Federal',
1058:'Órgão Público do Poder Legislativo Estadual ou do Distrito Federal',
1066:'Órgão Público do Poder Legislativo Municipal',
1074:'Órgão Público do Poder Judiciário Federal ',
1082:'Órgão Público do Poder Judiciário Estadual',
1104:'Autarquia Federal',
1112:'Autarquia Estadual ou do Distrito Federal',
1120:'Autarquia Municipal',
1139:'Fundação Pública de Direito Público Federal',
1147:'Fundação Pública de Direito Público Estadual ou do Distrito Federal',
1155:'Fundação Pública de Direito Público Municipal',
1163:'Órgão Público Autônomo Federal',
1171:'Órgão Público Autônomo Estadual ou do Distrito Federal',
1180:'Órgão Público Autônomo Municipal',
1198:'Comissão Polinacional',
1201:'Fundo Público',
1210:'Consórcio Público de Direito Público (Associação Pública)',
1228:'Consórcio Público de Direito Privado',
1236:'Estado ou Distrito Federal',
1244:'Município',
1252:'Fundação Pública de Direito Privado Federal',
1260:'Fundação Pública de Direito Privado Estadual ou do Distrito Federal',
1279:'Fundação Pública de Direito Privado Municipal',
2000:'Entidades Empresariais Sem Definição',
2011:'Empresa Pública',
2038:'Sociedade de Economia Mista',
2046:'Sociedade Anônima Aberta',
2054:'Sociedade Anônima Fechada',
2062:'Sociedade Empresária Limitada',
2070:'Sociedade Empresária em Nome Coletivo',
2089:'Sociedade Empresária em Comandita Simples',
2097:'Sociedade Empresária em Comandita por Ações',
2127:'Sociedade em Conta de Participação',
2135:'Empresário (Individual)',
2143:'Cooperativa',
2151:'Consórcio de Sociedades',
2160:'Grupo de Sociedades',
2178:'Estabelecimento, no Brasil, de Sociedade Estrangeira',
2194:'Estabelecimento, no Brasil, de Empresa Binacional Argentino-Brasileira',
2216:'Empresa Domiciliada no Exterior',
2224:'Clube/Fundo de Investimento',
2232:'Sociedade Simples Pura',
2240:'Sociedade Simples Limitada',
2259:'Sociedade Simples em Nome Coletivo',
2267:'Sociedade Simples em Comandita Simples',
2275:'Empresa Binacional',
2283:'Consórcio de Empregadores',
2291:'Consórcio Simples',
2305:'Empresa Individual de Responsabilidade Limitada (de Natureza Empresária)',
2313:'Empresa Individual de Responsabilidade Limitada (de Natureza Simples)',
2321:'Sociedade Unipessoal de Advogados',
2330:'Cooperativas de Consumo',
3000:'Entidades Sem Fins Lucrativos Sem Definição',
3034:'Serviço Notarial e Registral (Cartório)',
3069:'Fundação Privada',
3077:'Serviço Social Autônomo',
3085:'Condomínio Edilício',
3107:'Comissão de Conciliação Prévia',
3115:'Entidade de Mediação e Arbitragem',
3131:'Entidade Sindical',
3204:'Estabelecimento, no Brasil, de Fundação ou Associação Estrangeiras',
3212:'Fundação ou Associação Domiciliada no Exterior',
3220:'Organização Religiosa ',
3239:'Comunidade Indígena ',
3247:'Fundo Privado',
3255:'Órgão de Direção Nacional de Partido Político',
3263:'Órgão de Direção Regional de Partido Político',
3271:'Órgão de Direção Local de Partido Político',
3280:'Comitê Financeiro de Partido Político',
3298:'Frente Plebiscitária ou Referendária',
3301:'Organização Social (OS)',
3306:'Organização Social (OS)',
3310:'Demais Condomínios',
3999:'Associação Privada',
4000:'Pessoa Física',
4014:'Empresa Individual Imobiliária',
4022:'Segurado Especial',
4081:'Contribuinte individual',
4090:'Candidato a Cargo Político Eletivo',
4111:'Leiloeiro',
4124:'Produtor Rural (Pessoa Física)',
5010:'Organização Internacional',
5029:'Representação Diplomática Estrangeira',
5037:'Outras Instituições Extraterritoriais'})

cnes_estab['cod_natureza_agregada_label'] = cnes_estab.cod_natureza_agregada.map({
1:'Administração pública federal',
2:'Administração pública estadual',
3:'Administração pública municipal',
4:'Administração pública outros',
5:'Entidades empresariais',
6:'Entidades sem fins lucrativos',
7:'Pessoas físicas',
8:'Organizações internacionais e extraterritoriais'})

cnes_estab['cod_subtipo_label'] = cnes_estab.cod_subtipo.map({
0:'Não se aplica',
1:'Hospital especializado em pediatria',
2:'Hospital especializado em cardiologia',
3:'Hospital especializado em ortopedia',
4:'Hospital especializado em concologia',
5:'Hospital maternidade',
6:'Hospital especializado em psiquiatria',
7:'Centro Especializado em Reabilitação (CER)',
8:'Centro de Referência em Saúde do Trabalhador (CEREST)',
9:'Centro de Especialidades Odontológicas tipo I (CEO-I)',
10:'Centro de Especialidades Odontológicas tipo II (CEO-II)',
11:'Centro de Especialidades Odontológicas tipo III (CEO-III)',
12:'Outros centros de especialidades',
13:'Laboratório Regional de Prótese Dentária (LRDP)',
14:'Unidade móvel odontológica',
15:'Consultório itinerante',
16:'Unidade de vigilância de zoonoses',
17:'Laboratório Central de Saúde Pública (LACEN) Porte I',
18:'Laboratório Central de Saúde Pública (LACEN) Porte II',
19:'Laboratório Central de Saúde Pública (LACEN) Porte III',
20:'Laboratório Central de Saúde Pública (LACEN) Porte IV',
21:'Laboratório Central de Saúde Pública (LACEN) Porte V',
22:'Secretaria de Estado da Saúde (SES)',
23:'Regional de saúde',
24:'Secretaria Municipal de Saúde (SMS)',
25:'Distrito Sanitário',
26:'Sede de operadora de planos de saúde',
27:'Sede de consórcio público',
28:'Coordenador, regional ou núcleo de hemoterapia/hematologia',
29:'Unidade de Coleta de Transfusão (UCT)',
30:'Unidade de Coleta (UC)',
31:'Central de Triagem Laboratorial de Doadores (CTLD)',
32:'Agência Transfusional (AT)',
33:'Centro de Atenção Psicossocial tipo I (CAPS I)',
34:'Centro de Atenção Psicossocial tipo II (CAPS II)',
35:'Centro de Atenção Psicossocial tipo III (CAPS III)',
36:'Centro de Atenção Psicossocial Infanto-juvenil',
37:'Centro de Atenção Psicossocial Álcool e Drogas',
38:'Unidade de apoio indígena',
39:'Posto de saúde idígena',
40:'Polo-base tipo I',
41:'Polo-base tipo II',
42:'Distrito Sanitário Especial Indígena (DSEI)',
43:'Pronto Atendimento Geral',
44:'Pronto Atendimento Especializado',
45:'Unidade de Pronto Atendimento (UPA)',
46:'Unidade de Telessaúde',
47:'Central de regulação médica das urgências estadual',
48:'Central de regulação médica das urgências regional',
49:'Central de regulação médica das urgências municipal',
50:'Laboratório Central de Saúde Pública (LACEN)',
51:'Laboratório Federal',
52:'Laboratório Estadual',
53:'Laboratório Municipal',
54:'Central de regulação do acesso ambulatorial',
55:'Central de regulação do acesso internação hospitalar',
56:'Central de regulação do acesso ambulatorial e internação hospitalar',
57:'Central de regulação do acesso alta complexidade e ambulatorial',
58:'Central de regulação do acesso alta complexidade e internação hospitalar',
59:'Central de regulação do acesso alta complexidade, ambulatorial e internação hospitalar',
60:'Central de notificação, capacitação e distribuição de órgãos sede',
61:'Central de notificação, capacitação e distribuição de órgãos regional',
62:'Organização de procura de órgãos e tecidos'})

cnes_estab['cod_natureza_mantenedora_label'] = cnes_estab.cod_natureza_mantenedora.map({
1000:'Administração Pública Sem Definição',
1015:'Órgão Público do Poder Executivo Federal',
1023:'Órgão Público do Poder Executivo dos Estados ou do Distrito Federal',
1031:'Órgão Público do Poder Executivo Municipal',
1040:'Órgão Público do Poder Legislativo Federal',
1058:'Órgão Público do Poder Legislativo Estadual ou do Distrito Federal',
1066:'Órgão Público do Poder Legislativo Municipal',
1074:'Órgão Público do Poder Judiciário Federal',
1082:'Órgão Público do Poder Judiciário Estadual',
1104:'Autarquia Federal',
1112:'Autarquia Estadual ou do Distrito Federal',
1120:'Autarquia Municipal',
1139:'Fundação Pública de Direito Público Federal',
1147:'Fundação Pública de Direito Público Estadual ou do Distrito Federal',
1155:'Fundação Pública de Direito Público Municipal',
1163:'Órgão Público Autônomo Federal',
1171:'Órgão Público Autônomo Estadual ou do Distrito Federal',
1180:'Órgão Público Autônomo Municipal',
1198:'Comissão Polinacional',
1201:'Fundo Público',
1210:'Consórcio Público de Direito Público (Associação Pública)',
1228:'Consórcio Público de Direito Privado',
1236:'Estado ou Distrito Federal',
1244:'Município',
1252:'Fundação Pública de Direito Privado Federal',
1260:'Fundação Pública de Direito Privado Estadual ou do Distrito Federal',
1279:'Fundação Pública de Direito Privado Municipal',
2000:'Entidades Empresariais Sem Definição',
2011:'Empresa Pública',
2038:'Sociedade de Economia Mista',
2046:'Sociedade Anônima Aberta',
2054:'Sociedade Anônima Fechada',
2062:'Sociedade Empresária Limitada',
2070:'Sociedade Empresária em Nome Coletivo',
2089:'Sociedade Empresária em Comandita Simples',
2097:'Sociedade Empresária em Comandita por Ações',
2127:'Sociedade em Conta de Participação',
2135:'Empresário (Individual)',
2143:'Cooperativa',
2151:'Consórcio de Sociedades',
2160:'Grupo de Sociedades',
2178:'Estabelecimento, no Brasil, de Sociedade Estrangeira',
2194:'Estabelecimento, no Brasil, de Empresa Binacional Argentino-Brasileira',
2216:'Empresa Domiciliada no Exterior',
2224:'Clube/Fundo de Investimento',
2232:'Sociedade Simples Pura',
2240:'Sociedade Simples Limitada',
2259:'Sociedade Simples em Nome Coletivo',
2267:'Sociedade Simples em Comandita Simples',
2275:'Empresa Binacional',
2283:'Consórcio de Empregadores',
2291:'Consórcio Simples',
2305:'Empresa Individual de Responsabilidade Limitada (de Natureza Empresária)',
2313:'Empresa Individual de Responsabilidade Limitada (de Natureza Simples)',
2321:'Sociedade Unipessoal de Advogados',
2330:'Cooperativas de Consumo',
3000:'Entidades Sem Fins Lucrativos Sem Definição',
3034:'Serviço Notarial e Registral (Cartório)',
3069:'Fundação Privada',
3077:'Serviço Social Autônomo',
3085:'Condomínio Edilício',
3107:'Comissão de Conciliação Prévia',
3115:'Entidade de Mediação e Arbitragem',
3131:'Entidade Sindical',
3204:'Estabelecimento, no Brasil, de Fundação ou Associação Estrangeiras',
3212:'Fundação ou Associação Domiciliada no Exterior',
3220:'Organização Religiosa',
3239:'Comunidade Indígena',
3247:'Fundo Privado',
3255:'Órgão de Direção Nacional de Partido Político',
3263:'Órgão de Direção Regional de Partido Político',
3271:'Órgão de Direção Local de Partido Político',
3280:'Comitê Financeiro de Partido Político',
3298:'Frente Plebiscitária ou Referendária',
3301:'Organização Social (OS)',
3306:'Organização Social (OS)',
3310:'Demais Condomínios',
3999:'Associação Privada',
4000:'Pessoa Física',
4014:'Empresa Individual Imobiliária',
4022:'Segurado Especial',
4081:'Contribuinte individual',
4090:'Candidato a Cargo Político Eletivo',
4111:'Leiloeiro',
4124:'Produtor Rural (Pessoa Física)',
5010:'Organização Internacional',
5029:'Representação Diplomática Estrangeira',
5037:'Outras Instituições Extraterritoriais',
9999:'Não se aplica'})

cnes_estab['ind_natureza_label'] = cnes_estab.ind_natureza.map({
1:'Pública',
2:'Privada'})

cnes_estab['cod_gestao_label'] = cnes_estab.cod_gestao.map({
1:'Estadual',
2:'Municipal',
3:'Dupla',
4:'Sem Gestão'})

cnes_estab['ind_atencao_basica_label'] = cnes_estab.ind_atencao_basica.map({
0:'Não',
1:'Atenção Básica',
2:'Sem Informação'})

cnes_estab['ind_media_complexidade_label'] = cnes_estab.ind_media_complexidade.map({
0:'Não',
1:'Média complexidade',
2:'Sem Informação'})

cnes_estab['ind_alta_complexidade_label'] = cnes_estab.ind_alta_complexidade.map({
0:'Não',
1:'Alta complexidade',
2:'Sem Informação'})

cnes_estab['ind_internacao_label'] = cnes_estab.ind_internacao.map({
0:'Não',
1:'Internação',
2:'Sem Informação'})

cnes_estab['ind_ambulatorio_label'] = cnes_estab.ind_ambulatorio.map({
0:'Não',
1:'Ambulatorial',
2:'Sem Informação'})

cnes_estab['ind_sadt_label'] = cnes_estab.ind_sadt.map({
0:'Não',
1:'Serviço de apoio diagnóstico e terapêutico',
2:'Sem Informação'})

cnes_estab['ind_urgencia_label'] = cnes_estab.ind_urgencia.map({
0:'Nâo',
1:'Urgência',
2:'Sem Informação'})

cnes_estab['ind_vig_reg_label'] = cnes_estab.ind_vig_reg.map({
0:'Não',
1:'"Vigilância, regulação e outros',
2:'Sem Informação'})

"""### Parte 3 - Banco Integrado Carga Horária e Estabelecimento"""

# Faz o Merge dos Bancos

cnes = pd.merge(cnes_ch, cnes_estab, on='num_unidade', how='inner')

# Salva o Banco do Cnes
cnes.to_csv('/content/cnes_'+data_cnes+'.txt', sep='\t', index=False, encoding='latin-1')

cnes.head(15)

"""### Parte 4 - Vínculos em Equipes 
#### Omitido
###### Para pesquisas que precisam dados de vínculos e informações de Recursos Humanos
"""

cnes_vinc_equi = pd.read_csv('/content/drive/My Drive/Academico/Doctorat/Dados/rlEstabEquipeProf202006.csv', sep=';',
                            header=0, usecols=(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18),
                            names=['cod_mun','num_area','num_seq_equipe','num_profissional','num_unidade','cod_cbo',
                                   'ind_sus','cod_vinculo_total','num_microarea','dat_entrada','dat_desligamento',
                                   'num_cnes_outra_equipe','cod_mun_outra_equipe','num_area_outra_equipe','num_ch_complementar',
                                   'cod_cbo_ch_complementar','ind_equipe_minima','cod_mun_atuacao','dat_atualizacao'])

cnes_vinc_equi['cod_vinculo_total_label'] = cnes_vinc_equi.cod_vinculo_total.map({
0:'Não informado',
10000:'Vínculo empregatício sem tipo',
10100:'Estatutário sem subtipo (antigo)',
10101:'Estatutário efetivo, servidor próprio',
10102:'Estatutário efetivo, servidor cedido',
10200:'Empregado público celetista sem subtipo',
10201:'Emprego público, subtipo CLT',
10202:'Empregado público celetista próprio',
10203:'Empregado público celetista cedido',
10300:'Contrato por prazo determinado, sem subtipo',
10301:'Contrato por prazo determinado, público',
10302:'Contrato por prazo determinado, privado',
10400:'Cargo comissionado, sem subtipo',
10401:'Cargo comissionado, subtipo cargo comissionado não cedido',
10402:'Cargo comissionado, subtipo cargo comissionado cedido',
10403:'Cargo comissionado, servidor público próprio',
10404:'Cargo comissionado, servidor público cedido',
10405:'Cargo comissionado, sem vínculo com o setor público',
10500:'Celetista',
10501:'Celetista, subtipo contrato por OSCIP/OS',
10502:'Celetista, subtipo contrato por ONG',
10503:'Celetista, subtipo contrato por entidade filantrópica',
10504:'Celetista, subtipo contrato por rede privada',
20000:'Autônomo sem tipo',
20100:'Intermediado por OS',
20200:'Intermediado por OSCIP',
20300:'Intermediado por ONG',
20400:'Intermediado por entidade filantrópica',
20500:'Intermediado por empresa privada',
20600:'Consultoria',
20700:'RPA',
20800:'Intermediado por cooperativa',
20900:'Autônomo, Pessoa Jurídica',
21000:'Autônomo, Pessoa Física',
21100:'Autônomo, cooperado',
30000:'Cooperado',
40100:'Bolsa',
40200:'Contrato verbal/informal',
40300:'Proprietário',
50000:'Residente, sem subtipo',
50101:'Residente, próprio',
50102:'Residente, subsidiado por outro ente/entidade',
60000:'Estagiário, sem subtipo',
60101:'Estagiário, próprio',
60102:'Estagiário, subsidiado por outro ente/entidade',
70100:'Bolsista, Sem subtipo',
70101:'Bolsista, próprio',
70102:'Bolsista, subsidiado por outro ente/entidade',
80000:'Intermediado, sem subtipo',
80100:'Intermediado, empregado público celetista',
80200:'Intermediado, contrato temporário ou por prazo',
80300:'Intermediado, cargo comissionado',
80400:'Intermediado, celetista',
80501:'Intermediado, autônomo Pessoa Jurídica',
80502:'Intermediado, autônomo Pessoa Física',
80600:'Intermediado, cooperado',
90100:'Informal, contratado verbalmente',
90200:'Informal, voluntariado',
100100:'Servidor público cedido para iniciativa privada',
100200:'Empregado público celetista cedido para iniciativa privada',
100300:'Comissionado cedido para iniciativa privada'})

cnes_vinc_equi['cod_vinculacao_label'] = cnes_vinc_equi.cod_vinculacao.map({
1:'Vínculo empregatício',
2:'Autônomo',
3:'Estágio',
4:'Residência',
5:'Bolsa',
6:'Intermediado',
7:'Outros',
9:'Não informado'})

cnes_vinc_equi['cod_vinculo_label'] = cnes_vinc_equi.cod_vinculo.map({
0:'Vínculo empregatício sem tipo',
1:'Estatutário',
2:'Empregado público',
3:'Contrato por prazo determinado',
4:'Cargo comissionado',
5:'Celetista',
6:'Autônomo',
7:'Consultoria',
8:'RPA',
9:'Cooperado',
10:'Intermediado por OS (antigo)',
11:'Intermediado por OSCIP (antigo)',
12:'Intermediado por ONG (antigo)',
13:'Intermediado por entidade filantrópica (antigo)',
14:'Intermediado por empresa privada (antigo)',
15:'Bolsa',
16:'Residente',
17:'Estagiário',
18:'Contrato informal',
19:'Servidor público cedido para iniciativa privada',
99:'Não informado'})

cnes_vinc_equi['cod_subvinculo_label'] = cnes_vinc_equi.cod_subvinculo.map({
1:'Cargo Comissionado Cedido (antigo)',
2:'Cargo Comissionado Não Cedido (antigo)',
3:'Emprego público celetista (antigo)',
4:'Contrato por entidade filantrópica',
5:'Contrato por ONG',
6:'Contrato por OSCIP/OS',
7:'Contrato por rede privada',
8:'Estatutário efetivo, servidor próprio',
9:'Estatutário efetivo, servidor cedido',
10:'Empregado Público celetista próprio',
11:'Empregado Público celetista cedido',
12:'Contrato Temporário com a Administração Pública',
13:'Contrato por prazo determinado no setor privado',
14:'Cargo Comissionado, servidor público próprio',
15:'Cargo Comissionado, servidor público cedido',
16:'Cargo Comissionado, sem vínculo com o setor público',
17:'Residente próprio',
18:'Residente subsidiado por outro ente/entidade',
19:'Estagiário próprio',
20:'Estagiário subsidiado por outro ente/entidade',
21:'Bolsista próprio',
22:'Bolsista subsidiado por outro ente/entidade',
23:'Intermediado, empregado público celetista',
24:'Intermediado, contrato temporário',
25:'Intermediado, cargo comissionado',
26:'Intermediado, celetista',
27:'Intermediado, autônomo pessoa jurídica',
28:'Intermediado, autônomo pessoa física',
29:'Intermediado, cooperado',
30:'Informal, contratado verbalmente',
31:'Informal, voluntariado',
32:'Servidor público cedido para iniciativa privada',
33:'Empregado público celetista cedido para iniciativa privada',
34:'Comissionado cedido para iniciativa privada',
98:'Sem subtipo',
99:'Não informado'})

cnes_vinc_equi['cod_vinculo_agregado_label'] = cnes_vinc_equi.cod_vinculo_agregado.map({
1:'Estatutário',
2:'Empregado público',
3:'Comissionado',
4:'Celetista',
5:'Temporário',
6:'Autônomo, pessoa física',
7:'Autônomo, pessoa jurídica',
8:'Cooperado',
9:'Bolsista',
10:'Residente',
11:'Estagiário',
12:'Informal',
13:'Outros, vínculo empregatício sem descrição',
14:'Outros, autônomo sem descrição',
99:'Sem informação'})

cnes_vinc_equi['ind_protegido_label'] = cnes_vinc_equi.ind_protegido.map({
1:'Protegido',
2:'Desprotegido',
3:'Sem definição',
9:'Não informado',})

cnes_vinc_equi['ind_equipe_minima_label'] = cnes_vinc_equi.ind_equipe_minima.map({
0:'Profissional complementar',
1:'Profissional da equipe mínima'})

cnes_vinc_equi['cod_equipe_label'] = cnes_vinc_equi.cod_equipe.map({
1:'ESF',
2:'ESFSB M1',
3:'ESFSB M2',
4:'EACS',
5:'EPEN',
6:'ENASF1',
7:'ENASF2',
8:'EMSI',
9:'EMSIAL',
10:'EACSSB M1',
11:'EACSSB M2',
12:'ESFPR',
13:'ESFPRSB',
14:'ESFFSB',
15:'ESFFSB',
16:'EAB1',
17:'EAB2',
18:'EAB3',
19:'EAB1SB',
20:'EAB2SB',
21:'EAB3SB',
22:'EMAD',
23:'EMAP',
24:'ESF1',
25:'ESF1SB M1',
26:'ESF1SB M2',
27:'ESF2',
28:'ESF2SB M1',
29:'ESF2SB M2',
30:'ESF3',
31:'ESF3SB M1',
32:'ESF3SB M2',
33:'ESF4',
34:'ESF4SB M1',
35:'ESF4SB M2',
36:'ESFTRANS',
37:'ESFTRANSSB M1',
38:'ESFTRANSSB M2',
39:'ESFRSB M2',
40:'eCR M1',
41:'eCR M2',
42:'eCR M3',
43:'NASF1',
44:'NASF2',
45:'NASF3',
46:'EMAD2',
47:'ECD',
48:'EESE',
49:'EAP',
50:'EABP1',
51:'EABP1 com saúde mental',
52:'EABP2',
53:'EABP2 com saúde mental',
54:'EABP3'})

cnes_vinc_equi['cod_equipe_agregado_label'] = cnes_vinc_equi.cod_equipe_agregado.map({
1:'Equipe de saúde da família',
2:'Equipe de saúde da família com saúde bucal',
3:'Equipe de atenção básica',
4:'Equipe de saúde da família para população ribeirinha e fluvial',
5:'Equipe dos consultórios na rua',
6:'Equipe multidiciplinar de atenção domiciliar',
7:'Núcleo de Apoio à Saúde da Família',
8:'Outro'})

"""### Parte 5 - Equipamento

#### Omitido
###### Talves para pesquisas futuras sobre equipamentos
"""

cnes_equipamento = pd.read_csv('.csv', sep=';',
                              names=['num_unidade','cod_equipamento','cod_equipamento_tipo','qtd_equipamento', 'qtd_em_uso',
                                    'ind_disponivel_sus'], header=0)

cnes_equipamento.num_unidade.dropna(inplace=True)

cnes_equipamento.replace({6:8, 7:6, 8:7}, inplace=True)
cnes_equipamento.fillna({'np.NaN':3}, inplace=True) # Rever isso Aqui, ver se os missings estão sendo transofrmados

cnes_equipamento['cod_equipamento_label'] = cnes_equipamento.cod_equipamento.map({
1:'Câmara gama',
2:'Mamógrafo com comando simples',
3:'Mamógrafo com estereotaxia',
4:'Raio-X até 100ma',
5:'Raio-X mais de 100 a 500 ma',
6:'Raio-X mais de 500ma',
7:'Raio-X dentário',
8:'Raio-X com fluoroscopia',
9:'Raio-X para densitometria óssea',
10:'Raio-X para hemodinâmica',
11:'Tomógrafo computadorizado',
12:'Ressonância Magnética',
13:'Ultrassom doppler colorido',
14:'Ultrassom ecógrafo',
15:'Ultrassom convencional',
16:'Processadora de filme exclusiva para mamografia',
17:'Mamógrafo computadorizado',
18:'PET/CT',
21:'Controle ambiental/Ar condicionado central',
22:'Grupo gerador',
23:'Usina de oxigênio',
31:'Endoscópio das vias respiratórias',
32:'Endoscópio das vias urinárias',
33:'Endoscópio digestivo',
34:'Equipamentos para optometria',
35:'Laparoscópio/Vídeo',
36:'Microscópio cirúrgico',
37:'Cadeira oftalmológica',
38:'Coluna oftalmológica',
39:'Refrator',
40:'Lensometro',
41:'Eletrocardiógrafo',
42:'Eletroencefalógrafo',
44:'Projetor ou tabela de optótipos',
45:'Retinoscópio',
46:'Oftalmoscópio',
47:'Ceratômetro',
48:'Tonômetro de aplanaçao',
49:'Biomicroscópio (lâmpada de fenda)',
50:'Campímetro',
51:'Bomba/Balão intraaórtico',
52:'Bomba de infusão',
53:'Berco aquecido',
54:'Bilirrubinômetro',
55:'Debitômetro',
56:'Desfibrilador',
57:'Equipamento de fototerapia',
58:'Incubadora',
59:'Marcapasso temporário',
60:'Monitor de ECG',
61:'Monitor de pressão invasivo',
62:'Monitor de pressão não invasivo',
63:'Reanimador pulmonar tipo ambu',
64:'Respirador/ventilador',
71:'Aparelho de diatermia por ultrassom (ondas curtas)',
72:'Aparelho de eletroestimulação',
73:'Bomba de infusão de hemoderivados',
74:'Equipamentos de aférese',
75:'Equipamento para audiometria',
76:'Equipamento de circulação extracorpórea',
77:'Equipamento para hemodiálise',
78:'Forno de Bier',
80:'Equipo odontológico completo',
81:'Compressor odontológico',
82:'Fotopolimerizador',
83:'Caneta de alta rotação',
84:'Caneta de baixa rotação',
85:'Amalgamador',
86:'Aparelho de profilaxia com jato de bicarbonato',
87:'Emissões otoacústicas evocadas transientes',
88:'Emissões otoacústicas evocadas por produto de distorção',
89:'Potencial evocado auditivo de tronco encefálico automático',
90:'Potencial evocado auditivo de troco encefálico de curta, média e longa latência',
91:'Audiômetro de um canal',
92:'Audiômetro de dois canais',
93:'Imitanciômetro',
94:'Imitanciômetro multifrequencial',
95:'Cabine acústica',
96:'Sistema de campo livre',
97:'Sistema completo de reforço visual (VRA)',
98:'Ganho de inserção',
99:'Hi-Pro'})

cnes_equipamento['cod_equipamento_tipo_label'] = cnes_equipamento.cod_equipamento_tipo.map({
1:'Equipamentos de diagnóstico por imagem',
2:'Equipamentos de infraestrutura',
3:'Equipamentos por métodos ópticos',
4:'Equipamentos por métodos gráficos',
5:'Equipamentos para manutenção da vida',
6:'Equipamentos de odontologia',
7:'Equipamentos de audiologia',
8:'Outros equipamentos'})

cnes_equipamento['ind_disponivel_sus_label'] = cnes_equipamento.ind_disponivel_sus.map({
1:'Disponível para o SUS',
2:'Não disponível para o SUS',
3:'Não informado'})
