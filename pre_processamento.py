import codecs
import json
import base64
import os
import io
from PyPDF2 import PdfReader
import base64
from tqdm import tqdm
import re
import string
from unidecode import unidecode
import chardet
import pandas as pd
import nltk
import pickle
import os
import pytesseract
import PyPDF2
from PIL import Image
nltk.download("stopwords")
nltk.download('punkt')
nltk.download('rslp')
from nltk.corpus import stopwords

def list_files(folder_path, file_extension):
    """
    Gera informações sobre arquivos em um diretório com a extensão especificada.

    Parâmetros:
    - folder_path: Caminho para o diretório.
    - file_extension: Extensão de arquivo a ser filtrada.

    Yields:
    - Tuple contendo nome do arquivo, caminho completo do arquivo e índice.
    """
    for file_name in tqdm(os.listdir(folder_path)):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_path.endswith(file_extension):
            yield file_name, file_path
    


def read_json(arquivo_json):
    """
    Lê um arquivo JSON e retorna seu conteúdo.

    Parâmetros:
    - arquivo_json: Caminho do arquivo JSON.

    Retorna:
    - Dados lidos do arquivo JSON.
    """
    with open(arquivo_json, 'r', encoding='utf-8') as arquivo:
        dados = json.load(arquivo)
    return dados


def is_pdf_contains_text(pdf_path):
    """
    Verifica se um PDF contém texto.

    Parâmetros:
    - pdf_path: Caminho do arquivo PDF.

    Retorna:
    - True se o PDF contém texto, False caso contrário.
    """
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PdfReader(pdf_file)
        for page in pdf_reader.pages:
            if page.extract_text().strip():
                return True
        return False

# Função para extrair textos de arquivos PDF
def extract_text_from_pdf(pdf_path):
    """
    Extrai texto de um arquivo PDF.

    Parâmetros:
    - pdf_path: Caminho do arquivo PDF.

    Retorna:
    - Texto extraído do PDF.
    """
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PdfReader(pdf_file)
        text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
        return text


def extract_text_from_image(file_path):
    """
    Extrai texto de uma imagem.\n
    O arquivo é salvo em uma pasta 'temp', a operação de OCR é realizada\n 
    o texto é extraído e, ao final, o arquivo e a pasta são removidos.

    Parâmetros:
    - file_path: Caminho da imagem.

    Retorna:
    - Texto extraído da imagem.
    """
    # Criar uma pasta temporária para armazenar as imagens
    temp_folder = "temp"
    os.makedirs(temp_folder, exist_ok=True)

    pdf = PdfReader(file_path)
    page = pdf.pages[0]
    xObject = page['/Resources']['/XObject'].get_object()
    count = 0

    for obj in xObject:
        if xObject[obj]['/Subtype'] == '/Image':
            img_data = xObject[obj].get_data()
            img = Image.open(io.BytesIO(img_data))
            img_filename = os.path.join(temp_folder, f"image_{count}.png")
            img.save(img_filename, "PNG")
            count += 1

    # Realizar OCR nas imagens e extrair o texto
    extracted_text = ''
    for image_filename in os.listdir(temp_folder):
        image_path = os.path.join(temp_folder, image_filename)
        img = Image.open(image_path)
        text = pytesseract.image_to_string(img)
        extracted_text += text

    # Remover as imagens da pasta temporária
    for image_filename in os.listdir(temp_folder):
        image_path = os.path.join(temp_folder, image_filename)
        os.remove(image_path)

    os.rmdir(temp_folder)  # Remover a pasta temporária vazia
   
    return extracted_text

# Generator para extrair textos de arquivos PDF
def pdf_text_extractor(folder_path, file_extension):
    """
    Generator para extrair textos de arquivos PDF e imagens em um diretório.

    Parâmetros:
    - folder_path: Caminho para o diretório.
    - file_extension: Extensão de arquivo a ser filtrada.

    Yields:
    - Tuple contendo nome do arquivo truncado, texto extraído.
    """
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_path.endswith(file_extension):
            try:
                if file_extension == '.pdf' and is_pdf_contains_text(file_path):
                    text = extract_text_from_pdf(file_path)
                else:
                    text = extract_text_from_image(file_path)
                
                yield file_name[:25], text
                
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                print("=" * 50)

# conversor de texto para base64
def txt_to_base64(txt):
    """
    Converte texto para formato base64.

    Parâmetros:
    - txt: Texto a ser convertido.

    Retorna:
    - Texto convertido em base64.
    """
    text_base64 = base64.b64encode(txt.encode()).decode()
    return text_base64

# conversor de texto base64 para outra codificação
def decode_base64(texto_base64, encoding):
    """
    Decodifica dados em base64 para texto.

    Parâmetros:
    - texto_base64: Dados em base64 a serem decodificados.
    - encoding: Codificação alvo para a decodificação.

    Retorna:
    - Texto decodificado.
    """
    padding_needed = len(texto_base64) % 4
    if padding_needed != 0:
        padding = "=" * (4 - padding_needed)
        texto_base64 += padding

    # Decodificar o texto da base64 para bytes
    bytes_decodificados = base64.b64decode(texto_base64)

    try:
        # Decodificar os bytes para UTF-8
        texto_decodificado = bytes_decodificados.decode(encoding)
        return texto_decodificado
    except UnicodeDecodeError:
        return "Erro ao decodificar o texto"
    
# Processamento individual dos textos
def processamento_texto_corpus(txt):
    """
    Realiza o processamento de texto: tokenização, remoção de stopwords, acentos, numeros e pontuação.

    Parâmetros:
    - txt: Texto a ser processado.

    Retorna:
    - Lista de tokens após o processamento.
    """
    portugues_stops = stopwords.words('portuguese')
    tokens_regex = re.findall(r"\w+(?:'\w+)?|[^\w\s]", txt)
    tokens_sem_stop = [t for t in tokens_regex if t not in portugues_stops]
    tokens_sem_acentos = [unidecode(t) for t in tokens_sem_stop]
    tokens_sem_numeros = [re.sub(r'\d', '', t) for t in tokens_sem_acentos]
    tokens_sem_pontuacao = [t for t in tokens_sem_numeros if t not in string.punctuation]
    tokens = [t.lower() for t in tokens_sem_pontuacao]
    del tokens_regex, tokens_sem_acentos, tokens_sem_stop, tokens_sem_pontuacao
    return tokens


def recodificar_csv_para_utf8(arquivo_entrada, arquivo_saida):
    """
    Recodifica um arquivo CSV para o formato UTF-8.

    Parâmetros:
    - arquivo_entrada: Caminho do arquivo de entrada (CSV).
    - arquivo_saida: Caminho do arquivo de saída (UTF-8 recodificado).
    """
    def detectar_codificacao(arquivo):

        with codecs.open(arquivo, 'rb', encoding='utf-8') as f:
            resultado = chardet.detect(f.read())
        return resultado['encoding']

    codificacao = detectar_codificacao(arquivo_entrada)
    df = pd.read_csv(arquivo_entrada, encoding=codificacao)
    df.to_csv(arquivo_saida, index=False, encoding='utf-8')

# Criar conjunto de palavas com os nomes dos servidores
# Os dados estão no página do TRE-BA 'tabelas-de-lotacao-de-pessoal' no link
# https://www.tre-ba.jus.br/transparencia-e-prestacao-de-contas/relatorios-cnj/recursos-humanos-e-remuneracao/tabelas-de-lotacao-de-pessoal-tlp
# link: Relação Nominal dos Servidores por Unidade de Lotação
def cria_conjunto_nomes(arquivo):
    """
    Crie e sersializa um conjunto com os nomes dos servidores

    Parâmetros:
    - arquivo: arquivo csv com os nomes dos servidores.

    Retorna:
    - Conjunto de nomes.
    """
    df = pd.read_csv(arquivo, sep=';', encoding='utf-8')

    nomes = ''
    conjunto_nomes = set()  # Usamos um conjunto para evitar duplicatas
    
    for _, row in df.iterrows():
        if isinstance(row['NOME'], str) and row['NOME'] != '':
            nomes += ((row['NOME'].lower().split()) + ' ')
    conjunto_nomes = nomes
    
    # Serializar o conjunto usando pickle
    with open('conjunto_nomes.pkl', 'wb') as arquivo_serializado:
        pickle.dump(conjunto_nomes, arquivo_serializado)

    return conjunto_nomes

def carregar_conjunto_nomes(arquivo_serializado):
    """
    Retorna um conjunto de nomes serializado de um arquivo.

    Parâmetros:
    - arquivo_serializado: Caminho do arquivo contendo o conjunto serializado.

    Retorna:
    - Conjunto de nomes.
    """
    with open(arquivo_serializado, 'rb') as arquivo:
        conjunto_nomes = pickle.load(arquivo)
    return conjunto_nomes