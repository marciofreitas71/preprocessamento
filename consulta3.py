import psycopg2
import pre_processamento as pre


def extracao_entidades_nomeadas(nr_processo):

    def execute_query(connection, query, parameters):
        try:
            cursor = connection.cursor()
            cursor.execute(query, parameters)
            return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Erro ao executar a consulta: {error}")
            return None

    # Parâmetros de conexão ao banco de dados
    host = ""  # Insira o endereço do seu banco de dados
    database = ""
    user = ""
    password = ""
    port = 5999

    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )

        # Número do processo fornecido como entrada
        nr_processo = (nr_processo)[:25]
        

        # Consulta unificada para obter informações de processo, partes e documentos
        unified_query = """
            SELECT
                tcp.id_processo_trf,
                tcp.id_processo,
                tcp.ds_objeto,
                trf.id_proc_referencia,
                tcp.dt_autuacao,
                tcp.ds_classe_judicial_sigla,
                tcp.ds_orgao_julgador,
                tcp.nr_vara,
                tcp.nr_ano_eleicao,
                tcp.nm_pessoa_autor,
                tcp.ds_municipio,
                tcp.cd_estado,
                tpp.id_pessoa,
                tpp.in_participacao,
                ttp.ds_tipo_parte,
                tul.ds_nome,
                tul.ds_login,
                tpd.id_tipo_processo_documento,
                tpd.id_usuario_juntada,
                tpd.ds_nome_usuario_juntada,
                tpd.dt_juntada,
                ttpd.ds_tipo_processo_documento,
                tpdb.ds_extensao extensao,
                tpdb.nr_documento_storage caminho_storage,
                tpdb.in_valido
            FROM client.tb_cabecalho_processo tcp
            JOIN client.tb_processo_trf trf ON trf.id_processo_trf = tcp.id_processo_trf
            LEFT JOIN client.tb_processo_parte tpp ON tpp.id_processo_trf = tcp.id_processo_trf AND tpp.in_segredo = false
            LEFT JOIN client.tb_tipo_parte ttp ON ttp.id_tipo_parte = tpp.id_tipo_parte
            LEFT JOIN acl.tb_usuario_login tul ON tul.id_usuario = tpp.id_pessoa
            LEFT JOIN core.tb_processo_documento tpd ON tcp.id_processo = tpd.id_processo
            LEFT JOIN core.tb_tipo_processo_documento ttpd ON tpd.id_tipo_processo_documento = ttpd.id_tipo_processo_documento
            LEFT JOIN core.tb_processo_documento_bin tpdb ON tpdb.id_processo_documento_bin = tpd.id_processo_documento_bin
            WHERE
            tcp.nr_processo = %s
            AND tpd.in_ativo = true
            AND tpdb.in_valido = true
            ORDER BY tcp.id_processo
        """

        result = execute_query(connection, unified_query, (nr_processo,))

        
        tokens_a_eliminar = []
        
        if result:
            print("Resultado da Consulta Unificada:")
            for row in result:            
                # # print(row)
                # print()
                # print(f'id_processo_trf: {row[0]}')
                tokens_a_eliminar.append(row[0])
                # print(f'Zona: {row[7]}')
                tokens_a_eliminar.append(row[7])
                # print(f'Objeto: {row[2]}')
                if row[2].count(" - ") != 0:
                    nomes_objeto = row[2].split(" - ")
                    for nome_objeto in nomes_objeto:
                        tokens_a_eliminar.append(nome_objeto)
                # print(f"Partido: {(row[9])}")
                if row[9].count(" - ") != 0: 
                    nome_partido = (row[9]).split(" - ")
                    for item_nome_partido in nome_partido:
                        tokens_a_eliminar.append(item_nome_partido)
                # print(f'Cidade: {row[10]}')
                tokens_a_eliminar.append(row[10])
                # print(f'Estado: {row[11]}')
                tokens_a_eliminar.append(row[11])
                if row[15].count(" - ") != 0:
                    requerente =  (row[15]).split(" - ")
                    for req in requerente:
                        tokens_a_eliminar.append(req)
                tokens_a_eliminar.append(row[15])
                # print(f'CNPJ: {row[16]}')
                tokens_a_eliminar.append(row[16])
                # print(f'registro_documento: {row[19]}')
                tokens_a_eliminar.append(row[19])
                # print()
            
            

    except (Exception, psycopg2.Error) as error:
        print("Erro ao se conectar ao banco de dados:", error)

    finally:
        if connection:
            connection.close()

    lista_final = set(tokens_a_eliminar)

    set_final = set()

    for elemento in lista_final:
        if isinstance(elemento, str):
            palavras = pre.processamento_texto_corpus(elemento)
            set_final.update(palavras)
        else:
            set_final.add(elemento)

    return set_final