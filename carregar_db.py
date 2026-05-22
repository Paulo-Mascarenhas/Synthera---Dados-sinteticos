import pandas as pd
import sqlite3
import os

DB_NOME = 'synthera.db'

ARQUIVOS_PARA_CARREGAR = {
    'frotas': 'dados_frotas.xlsx',
    'rh': 'dados_rh.xlsx',
    'vendas': 'dados_vendas.xlsx'
}

def carregar_dados_para_sqlite():
    """Lê os arquivos Excel e os carrega como tabelas em um banco de dados SQLite."""
    
    # Cria uma conexão com o banco de dados (o arquivo será criado se não existir)
    conn = sqlite3.connect(DB_NOME)
    print(f"Conexão com o banco de dados '{DB_NOME}' estabelecida.")

    for nome_tabela, nome_arquivo in ARQUIVOS_PARA_CARREGAR.items():
        if os.path.exists(nome_arquivo):
            print(f"Carregando arquivo '{nome_arquivo}' para a tabela '{nome_tabela}'...")
            
            # Lê o arquivo Excel para um DataFrame do pandas
            df = pd.read_excel(nome_arquivo)
            
            # Escreve o DataFrame para uma tabela no banco de dados SQLite
            # if_exists='replace' apaga a tabela antiga e a substitui pela nova
            df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
            print(f"Tabela '{nome_tabela}' carregada com sucesso com {len(df)} linhas.")
        else:
            print(f"Aviso: Arquivo '{nome_arquivo}' não encontrado. Pulando.")

    conn.close()
    print("Processo concluído. Conexão com o banco de dados fechada.")

if __name__ == '__main__':
    carregar_dados_para_sqlite()