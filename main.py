# %%
import datetime
import firebase_admin
from firebase_admin import firestore, credentials
from google.cloud.firestore_v1.base_query import FieldFilter, And
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF

def obter_data():
  """Retorna a data atual no formato DDMMYY."""
  hoje = datetime.datetime.now()
  data_formatada = hoje.strftime('%d%m%y')
  return data_formatada

def obter_data_formatada():
  """Retorna a data atual no formato DD/MM/YYYY."""
  hoje = datetime.datetime.now()
  data_formatada_local = hoje.strftime('%d/%m/%Y')
  return data_formatada_local

def obter_hora():
  """Retorna a data atual no formato DD/MM/YYYY."""
  hora = datetime.datetime.now()
  hora_formatada = hora.strftime('%H:%M:%S')
  return hora_formatada

# %%
entradas = {
    'contrato': 20041754,
    'periodos': ['2406','2407','2408']
}

# %%
# Use a service account.
cred = credentials.Certificate('serviceAccountKey.json')

# Application Default credentials are automatically created.
firebase_admin.initialize_app(cred)     # Caso não funcione, inicializar o app dentro da variavel "app"
db = firestore.client()

# Note: Use of CollectionRef stream() is prefered to get()
def obter_documentos(periodo, contrato):
    docs = (
        db.collection("dbAusencias")
        .where(filter=FieldFilter("ocPeriodo", "==", periodo))
        .where(filter=FieldFilter("contrato", "==", contrato))
        .stream()
    )
    # Criando uma lista para armazenar os dados
    data = []

    # Iterando sobre os documentos e adicionando os dados à lista
    for doc in docs:
        data.append(doc.to_dict())

    return(data)

# %%
# Fazer uma requisição para cada período solicitado
documentos = []

for item in entradas['periodos']:
    documentos = documentos + obter_documentos(periodo=item, contrato=entradas['contrato'])

# %%
print(documentos)

# %%
# Criando o DataFrame
df = pd.DataFrame(documentos)

# %%
display(df)

# %%
# TRATAMENTO DOS DADOS

# Preenchendo valores vazios na coluna 'funcUni'
df['funcUni'] = df['funcUni'].str.strip()
df['funcUni'] = df['funcUni'].fillna('NÃO INFORMADO')
df['funcUni'] = df['funcUni'].replace('', 'NÃO INFORMADO')
df['funcUni'] = df['funcUni'].replace(' ', 'NÃO INFORMADO')

# Convertendo 'ocPeriodo' para um formato mais legível (opcional)
df['Periodo'] = pd.to_datetime(df['ocPeriodo'], format='%y%m').dt.strftime('%m-%Y')

# Agrupando e contando ocorrências por período
ocorrencias_por_periodo = df.groupby('Periodo').size().reset_index(name='Total de Ocorrências')

# Agrupando e contando ocorrências por tipo
ocorrencias_por_tipo = df.groupby('ocTipo').size().reset_index(name='Total de Ocorrências')

# Agrupando e contando ocorrências por empresa
ocorrencias_por_empresa = df.groupby('funcEmp').size().reset_index(name='Total de Ocorrências')

# Agrupando e contando ocorrências por unidade
ocorrencias_por_unidade = df.groupby('funcUni').size().reset_index(name='Total de Ocorrências')

# %%
# Obter todas as unidades
unidades = df['funcUni'].unique()
# Obter quantidade de cada ocorrência por unidade
qtd_tipo_unidade = {}

for item in unidades:
    qtd_tipo_unidade[item] = {
        'Abandono de posto': 0,
        'Atraso (Atestado/Justificado)': 0,
        'Atraso (Injustificado)': 0,
        'Falta (Atestada/Justificada)': 0,
        'Falta (Injustificada)': 0,
        'Saída antecipada': 0
    }

for index, row in df.iterrows():
    tipo = row['ocTipo']
    unidade = row['funcUni']

    if unidade in qtd_tipo_unidade:
        if tipo in qtd_tipo_unidade[unidade]:
            valor_atual = qtd_tipo_unidade[unidade][tipo]
            qtd_tipo_unidade[unidade][tipo] = valor_atual + 1
        else:
            qtd_tipo_unidade[unidade][tipo] =  1
    else:
        qtd_tipo_unidade[unidade] = {tipo: 1}

# %%
# Gráfico de barras por período
plt.figure(figsize=(10,10))
sns.barplot(x='Periodo', y='Total de Ocorrências', data=ocorrencias_por_periodo)
plt.title('Total de Ocorrências por Período')
plt.title('Total de Ocorrências por Período', fontsize=18, fontweight='bold')       # Aumentando o tamanho da fonte do título
plt.xlabel('Período', fontsize=15)                  # Aumentando o tamanho da fonte dos rótulos dos eixos
plt.ylabel('Total de Ocorrências', fontsize=15)
plt.xticks(fontsize=12)                             # Aumentando o tamanho da fonte das ticks dos eixos
plt.yticks(fontsize=12)
plt.savefig('assets/ocorrencias_por_periodo.png')
plt.close()

# Gráfico de pizza por tipo
plt.figure(figsize=(10,10))
plt.pie(ocorrencias_por_tipo['Total de Ocorrências'], 
        labels=ocorrencias_por_tipo['ocTipo'], 
        autopct='%1.1f%%', 
        textprops={'fontsize': 15},
        labeldistance=0.8)  # Aumenta o tamanho da fonte das porcentagens
plt.title('Percentual de Ocorrências por Tipo', fontsize=18, fontweight='bold')  # Aumenta o tamanho do título
plt.savefig('assets/ocorrencias_por_tipo.png')
plt.close()


# %%
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", "B", size=12)

# Adicionando o cabeçalho
pdf.image('assets\LogoReports.png', x=10, y=12, h=8)
pdf.cell(50, 8, txt="", align='C')
pdf.cell(90, 8, txt="OCORRÊNCIAS OPERACIONAIS", align='C')
pdf.set_font("Arial", size=8)
pdf.cell(50, 8, txt="{}".format(obter_data_formatada()), align='R', ln=1)
pdf.cell(50, 8, txt="", align='C')
pdf.cell(90, 8, txt="Junho/24 - Agosto/24", align='C')
pdf.cell(50, 8, txt="{}".format(obter_hora()), align='R', ln=1)
pdf.set_fill_color(0, 0, 0)
pdf.cell(190, 2, border=1, align='C', fill=True, ln=1)
pdf.cell(200, 115, txt="", ln=1, align='C')

# Adicionando o gráfico de barras
pdf.image('assets/ocorrencias_por_periodo.png', x=10, y=40, w=85)

# Adicionando o gráfico de pizza
pdf.image('assets/ocorrencias_por_tipo.png', x=105, y=40, w=85)

# Adicionando a tabela de ocorrências por empresa
pdf.set_font("Arial","B", size=10)
pdf.cell(190, 10, txt="Ocorrências por Empresa", ln=1, align='C')
pdf.set_font("Arial", size=8)
pdf.set_fill_color(0, 0, 0)
pdf.set_text_color(255, 255, 255)
pdf.cell(95, 7, txt="Empresa", border=1, align='C', fill=True)
pdf.cell(95, 7, txt="Total de ocorrências (período selecionado)", border=1, ln=1, fill=True, align='C')
pdf.set_text_color(0, 0, 0)
for index, row in ocorrencias_por_empresa.iterrows():
    pdf.cell(95, 5, txt=str(row['funcEmp']), border=1)
    pdf.cell(95, 5, txt=str(row['Total de Ocorrências']), border=1, ln=1, align='C')

# Adicionando a tabela de ocorrências por unidade
pdf.set_font("Arial","B", size=10)
pdf.cell(190, 10, txt="Ocorrências por Unidade", ln=1, align='C')
pdf.set_font("Arial", size=8)
pdf.set_fill_color(0, 0, 0)
pdf.set_text_color(255, 255, 255)
pdf.cell(50, 7, txt="Unidade", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Ab. Posto", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Atraso (A/J)", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Atraso (I)", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Falta (A/J)", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Falta (I)", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Saída Ant.", border=1, align='C', fill=True)
pdf.cell(20, 7, txt="Total", border=1, ln=1, fill=True, align='C')
pdf.set_text_color(0, 0, 0)
for index, row in ocorrencias_por_unidade.iterrows():
    pdf.set_font("Arial", "B", size=8)
    pdf.cell(50, 5, txt=str(row['funcUni']), border=1)
    pdf.set_font("Arial", size=8)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Abandono de posto']), align='C', border=1)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Atraso (Atestado/Justificado)']), align='C', border=1)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Atraso (Injustificado)']), align='C', border=1)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Falta (Atestada/Justificada)']), align='C', border=1)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Falta (Injustificada)']), align='C', border=1)
    pdf.cell(20, 5, txt=str(qtd_tipo_unidade[row['funcUni']]['Saída antecipada']), align='C', border=1)
    pdf.set_font("Arial", "B", size=8)
    pdf.cell(20, 5, txt=str(row['Total de Ocorrências']), border=1, ln=1, align='C')

# Salvando o PDF
pdf.output("relatorio_ocorrencias_{}.pdf".format(obter_data()))