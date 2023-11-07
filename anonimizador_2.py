import base64
import pandas as pd
import pre_processamento as pre
import os
import re
from PyPDF2 import PdfReader
import requests
from tqdm import tqdm
import pytesseract
import consulta3
import nltk
import string
from unidecode import unidecode


pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'


# Pasta com os textos a serem lidos
folder_path = 'D:\\datasets\\12193\\APROVACAO'

# Generator para extração dos textos dos arquivos .pdf
arquivos_pdf = pre.pdf_text_extractor(folder_path, '.pdf')

# Itera sobre arquivos .pdf, extrai os textos e processa os dados
arquivos = [arquivo for arquivo in pre.list_files(folder_path, '.pdf')]
total_arquivos = len(arquivos)

# cria uma lista de municipios utilizando a API do IBGE
df_municipios = pd.read_json(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/ba/municipios', encoding='utf-8')
    
lista_mun_temp = (df_municipios['nome']).tolist()
lista_municipios = pre.processamento_texto_corpus(" ".join(lista_mun_temp))
lista_municipios.sort()

print(set(lista_municipios))

expressoes = set()

for arquivo, texto in tqdm(arquivos_pdf, total=total_arquivos, desc='Lendo os arquivos pdf e extraindo os textos'):
      
    expressoes_temp = list(consulta3.extracao_entidades_nomeadas(arquivo))
    expressoes = set(expressoes_temp + lista_municipios)
    # texto_tokenizado = [token.lower() for token in texto.split() if token.lower() not in expressoes]
    texto_tokenizado = [token for token in pre.processamento_texto_corpus(texto) if token not in expressoes and len(token) > 2]
    
    # print(lista_municipios)
    print()
    print(expressoes_temp)
    print()
    print(texto_tokenizado)
    print()

    


