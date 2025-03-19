import json,os
import functions_framework

from operator_gssheet import geraDadosParaGCS

PATH_SHEET_SECRETMANAGER = os.getenv("PATH_SHEET_SECRETMANAGER_SHEET")
PATH_SHEET_BUCKETNAME = os.getenv("PATH_SHEET_BUCKETNAME")
PATH_SHEET_TEMP_LOCAL = os.getenv("PATH_SHEET_TEMP_LOCAL")


def getParamentosSheet(request):
    try:
        request_json = request.get_json()
        SHEET_ID    = request_json.get('SHEET_ID')
        SHEET_ABA   = request_json.get('SHEET_ABA')
        SHEET_RANGE = request_json.get('SHEET_RANGE')

        aviso = ""
        if SHEET_ID is None:
            aviso += (f"SHEET_ID is none.\n")
        if SHEET_ABA is None:
            aviso += (f"SHEET_ABA is none\n")
        if SHEET_RANGE is None:
            aviso += (f"SHEET_RANGE is none\n")
        if SHEET_ID and SHEET_ABA and SHEET_RANGE:
            print(f"PARAMENTROS: {SHEET_ID},{SHEET_ABA},{SHEET_RANGE}")
            return SHEET_ID,SHEET_ABA,SHEET_RANGE
        else:
            return f"Erro ao pegar os paramentos do sheet: {aviso}", 400
    except Exception as e:
        return f"Erro ao pegar os paramentos do sheet: {str(e)}", 400

def getParamentosBQ(request):
    try:
        request_json = request.get_json()
        BQ_TABELA_NOME   = request_json.get('BQ_TABELA_NOME')
        BQ_TABELA_SCHEMA = request_json.get('BQ_TABELA_SCHEMA',"")

        if BQ_TABELA_NOME and BQ_TABELA_SCHEMA:
            print(f"Paramentros: {BQ_TABELA_NOME},{BQ_TABELA_SCHEMA}")
            return BQ_TABELA_NOME,BQ_TABELA_SCHEMA
        else:
            return None
    except Exception as e:
        return f"Erro ao pegar os paramentos do bigquery: {str(e)}", 400

@functions_framework.http
def tranformarBigquery(request):
    # Obtém o método HTTP da requisição
    metodo = request.method
    
    if metodo == 'POST':
        # Recebe parâmetros via corpo da requisição
        try:
            SHEET_ID,SHEET_ABA,SHEET_RANGE = getParamentosSheet(request)
            BQ_TABELA_NOME,BQ_TABELA_SCHEMA= getParamentosBQ(request)

            tabela = geraDadosParaGCS(PATH_SHEET_SECRETMANAGER,SHEET_ID,SHEET_ABA,SHEET_RANGE,
                             PATH_SHEET_BUCKETNAME,PATH_SHEET_TEMP_LOCAL,BQ_TABELA_SCHEMA,
                             BQ_TABELA_NOME)
            return f"tabela criada com sucesso: {tabela}",200

        except Exception as e:
            return f"Erro ao processar o corpo da requisição: {str(e)}", 400
    else:
        return "Método HTTP não permitido.", 405
