# 1. IMPORTAÇÕES
import pandas as pd
from faker import Faker
import random
import json

# 2. CONFIGURAÇÃO INICIAL
# Aqui inicializamos o Faker e dizemos a ele para gerar dados no padrão brasileiro.
fake = Faker('pt_BR')

# Carrega as configurações do arquivo JSON
def carregar_config():
    """Carrega as configurações do arquivo config.json."""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Erro: Arquivo 'config.json' não encontrado. Certifique-se de que ele existe no mesmo diretório.")
        return None
    except json.JSONDecodeError:
        print("Erro: O arquivo 'config.json' contém um erro de formatação.")
        return None

config = carregar_config()

# 3. FUNÇÕES DE GERAÇÃO DE DADOS

def _criar_ativos(num_veiculos, num_motoristas, ano_fim_manut, config_marcas):
    """Cria a lista de veículos (frota) e a lista de motoristas."""
    print("   - Criando ativos (frota e motoristas)...")
    marcas_modelos = config_marcas
    
    # Filtro para gerar nomes de motoristas "normais", sem títulos.
    motoristas = []
    titulos_indesejados = ['Dr. ', 'Dra. ', 'Sr. ', 'Sra. ']
    while len(motoristas) < num_motoristas:
        nome = fake.name()
        if not any(titulo in nome for titulo in titulos_indesejados):
            motoristas.append(nome)
    
    frota = []
    for _ in range(num_veiculos):
        marca_selecionada = random.choice(list(marcas_modelos.keys()))
        veiculo_info = random.choice(marcas_modelos[marca_selecionada])
        veiculo = {
            'Placa': fake.license_plate(),
            'Marca': marca_selecionada,
            'Modelo': veiculo_info['modelo'],
            'Tipo_Veiculo': veiculo_info['tipo'],
            'Motorista_Principal': random.choice(motoristas),
            # O ano de fabricação é, no máximo, o último ano do período de manutenção.
            'Ano_Fabricacao': random.randint(2010, ano_fim_manut),
            'KM_Inicial': random.randint(5000, 80000)
        }
        frota.append(veiculo)
    return frota, motoristas

def _gerar_log_eventos(qtd_registros, frota, ano_inicio_manut, ano_fim_manut, config_manut, config_custos, config_tempo_parado, config_multiplicadores):
    """Gera uma lista de eventos de manutenção baseada nos ativos."""
    print("   - Gerando log de eventos de manutenção...")
    categorias_manutencao = config_manut['categorias']
    pesos_categorias = config_manut['pesos_categorias']
    detalhes_manutencao = config_manut['detalhes']
    log_manutencoes = []
    for _ in range(qtd_registros):
        veiculo_do_evento = random.choice(frota)

        # --- Lógica de Negócio para o Evento ---
        categoria = random.choices(categorias_manutencao, weights=pesos_categorias, k=1)[0]
        
        # SUGESTÃO #1: Custo Inteligente baseado na Categoria
        custo_range = config_custos[categoria]
        custo = random.uniform(custo_range[0], custo_range[1])

        # Aplica o multiplicador de custo baseado no tipo de veículo
        tipo_veiculo = veiculo_do_evento['Tipo_Veiculo']
        multiplicador = config_multiplicadores.get(tipo_veiculo, 1.0) # Usa 1.0 como padrão se o tipo não for encontrado
        custo *= multiplicador

        data_inicio = fake.date_between(start_date=pd.to_datetime(f'{ano_inicio_manut}-01-01'), end_date=pd.to_datetime(f'{ano_fim_manut}-12-31'))
        if categoria == 'Corretiva':
            tempo_range = config_tempo_parado['Corretiva']
        else:
            tempo_range = config_tempo_parado['Outros']
        tempo_parado_dias = random.randint(tempo_range[0], tempo_range[1])
        data_fim = data_inicio + pd.Timedelta(days=tempo_parado_dias)

        linha_log = {
            'Placa': veiculo_do_evento['Placa'],
            'Marca': veiculo_do_evento['Marca'],
            'Modelo': veiculo_do_evento['Modelo'],
            'Tipo_Veiculo': veiculo_do_evento['Tipo_Veiculo'],
            'Ano_Fabricacao': veiculo_do_evento['Ano_Fabricacao'],
            'Data_Manutencao': data_inicio,
            'Data_Fim_Manutencao': data_fim,
            'Tempo_Parado_Dias': tempo_parado_dias,
            'Tipo_Manutencao': random.choice(detalhes_manutencao),
            'Categoria_Manutencao': categoria,
            'Custo_Manutencao_R$': round(custo, 2),
            'Motorista_Principal': veiculo_do_evento['Motorista_Principal'],
            # Coluna temporária para cálculo do KM
            'KM_Inicial_Veiculo': veiculo_do_evento['KM_Inicial']
        }
        log_manutencoes.append(linha_log)
    return log_manutencoes

def _processar_dataframe_final(log_manutencoes, ano_inicio_manut, config_simulacao):
    """Converte a lista de logs em um DataFrame e aplica pós-processamento e KPIs."""
    print("   - Processando dados e calculando KPIs...")
    if not log_manutencoes:
        return pd.DataFrame()
    
    df = pd.DataFrame(log_manutencoes)
    if df.empty:
        return df

    df['Data_Manutencao'] = pd.to_datetime(df['Data_Manutencao'])
    df = df.sort_values(by=['Placa', 'Data_Manutencao'])
    
    # --- Lógica de KM Inteligente ---
    # 1. Calcula a data do evento anterior. Para o primeiro evento, usa o início da simulação.
    data_inicio_simulacao = pd.to_datetime(f'{ano_inicio_manut}-01-01')
    df['data_anterior'] = df.groupby('Placa')['Data_Manutencao'].shift()
    df['data_anterior'] = df['data_anterior'].fillna(data_inicio_simulacao)
    
    # 2. Calcula os dias passados e o incremento de KM proporcional.
    df['dias_passados'] = (df['Data_Manutencao'] - df['data_anterior']).dt.days
    km_range = config_simulacao['km_diario_range']
    df['incremento_km'] = df['dias_passados'].apply(lambda dias: max(0, dias) * random.randint(km_range[0], km_range[1]))

    df['KM_Atual'] = df.groupby('Placa')['incremento_km'].cumsum() + df['KM_Inicial_Veiculo']
    
    df['Custo_Acumulado_Veiculo_R$'] = df.groupby('Placa')['Custo_Manutencao_R$'].cumsum()

    df = df.drop(columns=['incremento_km', 'KM_Inicial_Veiculo', 'data_anterior', 'dias_passados'])
    colunas_ordenadas = [
        'Placa', 'Marca', 'Modelo', 'Tipo_Veiculo', 'Ano_Fabricacao', 
        'Data_Manutencao', 'Data_Fim_Manutencao', 'Tempo_Parado_Dias', 
        'KM_Atual', 'Tipo_Manutencao', 'Categoria_Manutencao', 
        'Custo_Manutencao_R$', 'Custo_Acumulado_Veiculo_R$', 
        'Motorista_Principal'
    ]
    
    return df[colunas_ordenadas]

def gerar_dados_frotas(qtd_registros, num_veiculos, num_motoristas, ano_inicio_manut, ano_fim_manut):
    """
    Orquestra a criação do dataset de log de frotas.
    1. Carrega as configurações.
    2. Cria os ativos (frota, motoristas).
    3. Gera os eventos de manutenção.
    4. Processa os dados em um DataFrame final com KPIs.
    """
    if not config:
        print("Abortando execução devido a erro na configuração.")
        return None

    # Acessa o sub-dicionário de configurações de frotas
    config_frotas = config['frotas']

    # PASSO 1: Criar os ativos da empresa
    frota, motoristas = _criar_ativos(num_veiculos, num_motoristas, ano_fim_manut, config_frotas['marcas_modelos'])
    # PASSO 2: Gerar o log de eventos de manutenção
    log_manutencoes = _gerar_log_eventos(
        qtd_registros, frota, 
        ano_inicio_manut, ano_fim_manut, 
        config_frotas['manutencao'], 
        config_frotas['custos'], 
        config_frotas['tempo_parado_dias'], 
        config_frotas['custo_multiplicadores']
    )
    # PASSO 3: Pós-processamento e criação do DataFrame
    df_final = _processar_dataframe_final(log_manutencoes, ano_inicio_manut, config_frotas['simulacao'])
    return df_final

def gerar_dados_rh(num_funcionarios, anos_de_historico):
    """Gera um DataFrame com dados de funcionários para a área de RH."""
    if not config:
        print("Abortando execução devido a erro na configuração.")
        return None

    print("Iniciando geração de dados de RH...")
    
    config_rh = config['rh']
    ano_fim = pd.Timestamp.now().year
    ano_inicio = ano_fim - anos_de_historico + 1

    lista_funcionarios = []
    titulos_indesejados = ['Dr. ', 'Dra. ', 'Sr. ', 'Sra. ']
    for i in range(num_funcionarios):
        departamento = random.choice(config_rh['departamentos'])
        
        # Seleciona o cargo baseado em pesos para criar uma estrutura de pirâmide
        cargos_info = config_rh['cargos'][departamento]
        lista_cargos = [c['cargo'] for c in cargos_info]
        lista_pesos = [c['peso'] for c in cargos_info]
        cargo = random.choices(lista_cargos, weights=lista_pesos, k=1)[0]

        salario_range = config_rh['faixas_salariais'][cargo]
        salario = round(random.uniform(salario_range[0], salario_range[1]), 2)

        data_admissao = fake.date_between(start_date=pd.to_datetime(f'{ano_inicio}-01-01'), end_date=pd.to_datetime(f'{ano_fim}-12-31'))

        # Lógica de turnover mais robusta
        if random.random() < config_rh['turnover_chance']:
            # Converte a data de admissão (datetime.date) para Timestamp para permitir cálculos
            data_admissao_ts = pd.to_datetime(data_admissao)

            # Define a data mínima para demissão (30 dias após admissão)
            data_demissao_min_ts = data_admissao_ts + pd.Timedelta(days=30)
            data_fim_periodo_ts = pd.to_datetime(f'{ano_fim}-12-31')

            # Verifica se há um intervalo válido para a demissão
            if data_demissao_min_ts < data_fim_periodo_ts:
                status = 'Inativo'
                # Passa objetos datetime.date para o Faker, que é mais seguro
                data_demissao = fake.date_between(start_date=data_demissao_min_ts.date(), end_date=data_fim_periodo_ts.date())
            else:
                # Não há tempo hábil para demitir dentro do período
                status = 'Ativo'
                data_demissao = pd.NaT
        else:
            status = 'Ativo'
            data_demissao = pd.NaT

        nome_completo = fake.name()
        while any(titulo in nome_completo for titulo in titulos_indesejados):
            nome_completo = fake.name()

        funcionario = {
            'ID_Funcionario': 1000 + i,
            'Nome_Completo': nome_completo,
            'Data_Nascimento': fake.date_of_birth(minimum_age=18, maximum_age=65),
            'Gênero': random.choice(['Masculino', 'Feminino', 'Outro']),
            'Departamento': departamento,
            'Cargo': cargo,
            'Salario_Mensal_R$': salario,
            'Data_Admissao': data_admissao,
            'Data_Demissao': data_demissao,
            'Status': status
        }
        lista_funcionarios.append(funcionario)

    df_rh = pd.DataFrame(lista_funcionarios)
    print("Geração de dados de RH concluída.")
    return df_rh
