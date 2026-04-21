import pandas as pd
from flask import Flask, render_template, request, send_file
import io

# Importa a nossa lógica de geração de dados do outro arquivo
import gerador

app = Flask(__name__)

@app.route('/')
def index():
    """Renderiza a página inicial com o formulário."""
    return render_template('index.html')

@app.route('/gerar', methods=['POST'])
def gerar_arquivo():
    """Recebe os dados do formulário, gera o arquivo e o envia para download."""
    try:
        # 1. Coleta os dados comuns do formulário
        area = request.form.get('area')
        num_ativos = int(request.form.get('num_ativos'))
        anos_de_historico = int(request.form.get('anos_de_historico'))
        nome_arquivo = request.form.get('nome_arquivo')
        df_final = None

        if not nome_arquivo.endswith('.xlsx'):
            nome_arquivo += '.xlsx'

        # 2. Roteamento: chama a função geradora correta baseada na área
        if area == 'frotas':
            num_motoristas = int(request.form.get('num_motoristas'))
            qtd_registros = int(request.form.get('qtd_registros'))
            
            ano_fim_manut = pd.Timestamp.now().year
            ano_inicio_manut = ano_fim_manut - anos_de_historico + 1

            print(f"Iniciando geração para Frotas: {nome_arquivo}")
            df_final = gerador.gerar_dados_frotas(
                qtd_registros, num_ativos, num_motoristas, ano_inicio_manut, ano_fim_manut
            )
        elif area == 'rh':
            print(f"Iniciando geração para RH: {nome_arquivo}")
            df_final = gerador.gerar_dados_rh(num_ativos, anos_de_historico)

        if df_final is None:
            return "Área selecionada não é válida ou ocorreu um erro na geração.", 400

        # 3. Salva o DataFrame em um buffer de memória em vez de um arquivo físico
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False)
        output.seek(0)

        print("Geração concluída. Enviando arquivo para download.")
        # 4. Envia o buffer como um arquivo para o navegador
        return send_file(
            output,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return "Ocorreu um erro ao gerar o arquivo. Verifique o console do servidor.", 500

if __name__ == '__main__':
    # Inicia o servidor web de desenvolvimento
    app.run(debug=True, port=5001)