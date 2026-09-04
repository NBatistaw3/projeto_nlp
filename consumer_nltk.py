# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 19:47:26 2026

@author: nathi
"""

#ANALISE LEXICAL

import nltk
import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
import json
import os
from fpdf import FPDF
from confluent_kafka import Consumer


# Configuração Kafka
configuracao = {
    'bootstrap.servers': 'pkc-ldjyd.southamerica-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'V4AMWX4IVJVS3G5H',
    'sasl.password': 'cfltp87l9DjeTKJbXYc5qpfyHPAEOyD1PE/TToCpNCPCkR0cYDc2GCLuEIprDEyg',
    'group.id': 'anlise-nltk1',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}

consumer = Consumer(configuracao)
consumer.subscribe(['topic_1'])

def gerar_pdf_relatorio(id_analise, texto_original, resultados, metricas, caminho_matriz,
                        caminho_topicos=None, caminho_barras=None, caminho_hierarquia=None,
                        caminho_lexical=None, d_lex=None, df_pos=None):
    nome_arquivo = f"relatorio_{id_analise}.pdf"
    pdf = FPDF()
    pdf.add_page()

    # Título Principal
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, f"Relatório - Análise de Conteúdo- ID {id_analise}", ln=True, align='C')
    pdf.ln(5)

    # 1. Resumo do Léxico
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "1. Classificação por Léxico", ln=True)
    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 8, f"- Total de frases: {len(resultados)}", ln=True)
    pdf.cell(0, 8, f"- Negativas: {resultados.count('Negativo')} | Positivas: {resultados.count('Positivo')} | Neutras: {resultados.count('Neutro')}", ln=True)

    # 2. Performance do BERT
    pdf.ln(5)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "2. Métricas Técnicas por Categoria:", ln=True)
    pdf.set_font("Arial", "", 9)

    categorias = ["negativo", "positivo", "neutro"]
    for cat in categorias:
        f1 = metricas.get(f'f1_{cat}', 0)
        prec = metricas.get(f'precision_{cat}', 0)
        rec = metricas.get(f'recall_{cat}', 0)
        pdf.cell(0, 7, f"- {cat.upper()}: F1-Score: {f1:.2%} | Precisão: {prec:.2%} | Recall: {rec:.2%}", ln=True)
    
    pdf.ln(2)
    pdf.set_font("Arial", "B", 10)
    pdf.cell(0, 8, f"ACURÁCIA GERAL: {metricas.get('accuracy', 0):.2%}", ln=True)

    # 3.Matriz de Confusão
    if os.path.exists(caminho_matriz):
        pdf.ln(5)
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "3. Matriz de Confusão (Visual):", ln=True)
        pdf.image(caminho_matriz, x=20, y=pdf.get_y(), w=170)
        pdf.ln(90)

    # Bloco BERTopic
    if (caminho_topicos and os.path.exists(caminho_topicos)) or (caminho_barras and os.path.exists(caminho_barras)):
        pdf.add_page()
        
        # Inserção do Mapa de Distância
        if caminho_topicos and os.path.exists(caminho_topicos):
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "4. Mapa de Distancia Intertopica:", ln=True)
            pdf.image(caminho_topicos, x=15, w=180)
            pdf.ln(5) 

        # Barras
        if caminho_barras and os.path.exists(caminho_barras):
            pdf.ln(10)
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "5. Palavras-Chave por Topico:", ln=True)
            pdf.image(caminho_barras, x=10, w=180)
        
        if caminho_hierarquia and os.path.exists(caminho_hierarquia):
            pdf.add_page()
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "4.1 Hierarquia de Tópicos (Agrupamento):", ln=True)
            pdf.image(caminho_hierarquia, x=15, w=180)
            pdf.ln(5)

    # Bloco NLTK 
    if caminho_lexical and os.path.exists(caminho_lexical):
        pdf.add_page()
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "5. Análise Léxica (NLTK)", ln=True, align='C')
        pdf.image(caminho_lexical, x=20, w=170)
        pdf.ln(110)
        
    if d_lex:
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(0, 10, "Análise de Conteúdo (Metodologia de Bardin)", ln=True)

        pdf.set_font("Arial", '', 11)
        total_o = d_lex.get('total_o', 0)
        total_v = d_lex.get('total_v', 0)
        indice_ov = d_lex.get('indice_ov', 0)

        pdf.set_font("Arial", 'B', 11)
        pdf.cell(0, 10, f"Coeficiente de Riqueza de Vocabulário (O/V): {indice_ov:.4f}", ln=True) 

        pdf.set_font("Arial", 'I', 10)
        pdf.cell(0, 8, f"Massa Total de Ocorrências (O): {total_o}", ln=True)
        pdf.cell(0, 8, f"Inventário de Vocábulos (V): {total_v}", ln=True)
        pdf.multi_cell(0, 5, "Nota: O coeficiente O/V mensura a riqueza do vocabulário. "
                     "Valores mais elevados indicam maior reiteração de termos "
                     "específicos dentro do universo do discurso analisado.")
        
    if df_pos is not None:
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "6. Tabela de Classes Gramaticais (POS Tagging):", ln=True)
        pdf.set_font("Arial", "", 10)
        
        # Criando as linhas da tabela no PDF
        for index, row in df_pos.iterrows():
            classe = str(row['Classe'])
            freq = str(row['Frequencia'])
            pdf.cell(100, 8, f"{row['Classe']}", border=1)
            pdf.cell(40, 8, f"{row['Frequencia']}", border=1, ln=True)
            
# 7. Amostra do Texto (Final do Relatório)
    pdf.ln(10)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "7. Amostra do Conteúdo Processado:", ln=True)
    pdf.set_font("Arial", "", 9)
    
    # LIMPEZA CRÍTICA: Remove o \ufeff e garante que cabe no latin-1
    texto_seguro = texto_original.replace('\ufeff', '').encode('latin-1', 'replace').decode('latin-1')
    pdf.multi_cell(0, 5, txt=texto_seguro[:1000] + "...")

    pdf.output(nome_arquivo)
    return nome_arquivo

def verificar_e_unificar_pdf(id_analise, texto_bruto):
    arquivo_bert = f"temp_bert_{id_analise}.json"
    arquivo_lex  = f"temp_lex_{id_analise}.json"

    # Só prossegue se as duas partes já salvaram seus arquivos
    if os.path.exists(arquivo_bert) and os.path.exists(arquivo_lex):
        print(f"🔗 Unificando dados para o PDF final do ID {id_analise}...")
        
        try:
            with open(arquivo_bert, 'r') as f: d_bert = json.load(f)
            with open(arquivo_lex, 'r') as f: d_lex = json.load(f)

            # Converte o dicionário do NLTK de volta para DataFrame
            import pandas as pd
            df_pos_final = pd.DataFrame.from_dict(d_lex["tabela_pos"])

            caminho_pdf = gerar_pdf_relatorio(
                id_analise=id_analise,
                texto_original=texto_bruto,
                resultados=d_bert["resultados_sentimentos"],
                metricas=d_bert["metricas"],
                caminho_matriz=d_bert["imagem_matriz"],
                caminho_topicos=d_bert.get("imagem_topicos"), 
                caminho_barras=d_bert.get("imagem_barras"),
                caminho_hierarquia=d_bert.get("imagem_hierarquia"),
                caminho_lexical=d_lex["imagem_lex"],
                d_lex=d_lex,
                df_pos=df_pos_final                            
            )
            
            # Limpeza
            os.remove(arquivo_bert)
            os.remove(arquivo_lex)
            print(f"✅ RELATÓRIO FINAL GERADO COM SUCESSO: {caminho_pdf}")

        except Exception as e:
            print(f"⚠️ Erro na unificação do PDF para ID {id_analise}: {e}")


#NLTK
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

def limpar_texto_lexico(texto):
    texto = texto.lower()
    texto = re.sub(r'\{[^}]*?\}', '', texto)
    texto = re.sub(r'[^\w\s]', '', texto)
    
    stops = set(stopwords.words('portuguese'))
    stops_adicionais = {
        'vamos', 'porque', 'gente', 'sabe', 'coisa', 'aqui', 
        'hoje', 'cada', 'tempo', 'mundo', 'milhões', 'voltar', 
        'vez', 'vai', 'fazer', 'pode', 'ser', 'ter'
    }
    stops.update(stops_adicionais)
    
    tokens = word_tokenize(texto)
    return [t for t in tokens if t not in stops and len(t) > 2 and t.isalpha()]

def traduzir_pos(tag):
    mapeamento = {
        # Substantivos
        'NN': 'Noun (Singular)', 
        'NNS': 'Noun (Plural)',
        'NNP': 'Proper Noun (Singular)', 
        'NNPS': 'Proper Noun (Plural)',
        
        # Verbos
        'VB': 'Verb (Base Form)', 
        'VBD': 'Verb (Past Tense)', 
        'VBG': 'Verb (Gerund/Present Participle)',
        'VBN': 'Verb (Past Participle)', 
        'VBP': 'Verb (Non-3rd Person Singular Present)', 
        'VBZ': 'Verb (3rd Person Singular Present)',
        
        # Adjetivos
        'JJ': 'Adjective', 
        'JJR': 'Adjective (Comparative)', 
        'JJS': 'Adjective (Superlative)',
        
        # Advérbios
        'RB': 'Adverb', 
        'RBR': 'Adverb (Comparative)', 
        'RBS': 'Adverb (Superlative)',
        
        # Outros
        'IN': 'Preposition or Subordinating Conjunction', 
        'CD': 'Cardinal Number', 
        'PRP': 'Personal Pronome',
        'MD': 'Modal Verb',
        'FW': 'Foreign Word'
    }
    return mapeamento.get(tag, f"Other ({tag})")


# --- DENTRO DO LOOP WHILE DO KAFKA ---
while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    print("🔔 MENSAGEM RECEBIDA!")

    dados = json.loads(msg.value().decode('utf-8'))
    texto_bruto = dados.get("texto", "")
    id_analise = dados.get("cliente_id", "sem_id")

    # --- ANALISE LEXICAL (NLTK) ---
    tokens_lex = limpar_texto_lexico(texto_bruto)

    # 1. POS TAGGING (Contagem de Classes)
    tags = nltk.pos_tag(tokens_lex)
    contagem_pos = Counter([tag for word, tag in tags])
    # Criamos o DataFrame para a tabela do PDF
    df_pos_lex = pd.DataFrame(contagem_pos.items(), columns=['Classe', 'Frequencia'])
    df_pos_lex = df_pos_lex.sort_values('Frequencia', ascending=False)
    
    tags_raw = nltk.pos_tag(tokens_lex)
    contagem_pos = Counter([traduzir_pos(tag) for word, tag in tags_raw])
    
    df_pos_lex = pd.DataFrame(contagem_pos.items(), columns=['Classe', 'Frequencia'])
    df_pos_lex = df_pos_lex.sort_values('Frequencia', ascending=False)

 # 2. BAG OF WORDS (Frequência e Gráfico)
    contagem_freq = Counter(tokens_lex)
    df_freq_lex = pd.DataFrame(contagem_freq.most_common(20), columns=['Palavra', 'Frequencia'])
    
    nome_grafico_lex = f"freq_lexical_{id_analise}.png"
    plt.figure(figsize=(12, 7))
    # Usando a paleta 'magma' para diferenciar visualmente dos gráficos do BERT
    sns.barplot(data=df_freq_lex, x='Frequencia', y='Palavra', palette='magma')
    plt.title(f"Top 20 Palavras Mais Frequentes - ID {id_analise}")
    plt.savefig(nome_grafico_lex)
    plt.close()

  #3 Calculo de Bardin O/V
  
    total_o = len(tokens_lex)
    total_v = len(contagem_freq.keys())
    indice_ov = total_o / total_v if total_v > 0 else 0

#3. SALVAMENTO TEMPORÁRIO PARA UNIFICAÇÃO
    dados_lexical = {
        "id": id_analise,
        "tabela_pos": df_pos_lex.to_dict(orient="records"),
        "imagem_lex": nome_grafico_lex,
        "total_o": total_o,
        "total_v": total_v,
        "indice_ov": indice_ov
        }
    
    with open(f"temp_lex_{id_analise}.json", "w") as f:
        json.dump(dados_lexical, f)

verificar_e_unificar_pdf(id_analise, texto_bruto)
consumer.commit()

