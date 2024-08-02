# %%
import datetime
import firebase_admin
from firebase_admin import firestore, credentials
from google.cloud.firestore_v1.base_query import FieldFilter, Or
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
# Use a service account.
cred = credentials.Certificate('serviceAccountKey.json')

# Application Default credentials are automatically created.
firebase_admin.initialize_app(cred)     # Caso não funcione, inicializar o app dentro da variavel "app"
db = firestore.client()

# Note: Use of CollectionRef stream() is prefered to get()
docs = (
    db.collection("dbAusencias")
    .where(filter=FieldFilter("ocPeriodo", "==", "2405"))
    .stream()
)

# %%
# Criando uma lista para armazenar os dados
data = []

# Iterando sobre os documentos e adicionando os dados à lista
for doc in docs:
    data.append(doc.to_dict())

# Criando o DataFrame
df = pd.DataFrame(data)

# %%
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
# Obter quantidade de cada ocorrência por unidade
unidades = df['ocTipo'].unique()
dados_tabela = {}

for unidade in unidades:
    qtd_abandono = 0
    qtd_atraso_aj = 0
    qtd_atraso_i = 0
    qtd_falta_aj = 0
    qtd_falta_i = 0
    qtd_saida = 0
    print(unidade)

# %%
print(ocorrencias_por_unidade)

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
pdf.cell(90, 8, txt="Maio/24", align='C')
pdf.cell(50, 8, txt="{}".format(obter_hora()), align='R')
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
    pdf.cell(50, 5, txt=str(row['funcUni']), border=1)
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt="")
    pdf.cell(20, 5, txt=str(row['Total de Ocorrências']), border=1, ln=1, align='C')

# Salvando o PDF
pdf.output("relatorio_ocorrencias_{}.pdf".format(obter_data()))



