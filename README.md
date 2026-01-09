# ARGOS - Sistema de Infrações GESSUPER

Sistema de Download e Análise Exploratória de infrações fiscais do GESSUPER, desenvolvido para a **Receita Estadual de Santa Catarina**.

## Sobre o Projeto

O **Operação ARGOS** é uma aplicação web construída com [Streamlit](https://streamlit.io/) que permite a análise de infrações fiscais relacionadas a NFC-e (Nota Fiscal de Consumidor Eletrônica) e Cupons Fiscais (ECF). O sistema utiliza Inteligência Artificial para classificar e identificar possíveis irregularidades tributárias.

### Principais Funcionalidades

- **Ranking de Empresas**: Visualização das empresas com maiores valores de infrações identificadas
- **Consulta por CNPJ/IE**: Busca detalhada de infrações por empresa
- **Pesquisa de Produtos**: Busca produtos pela descrição para analisar tributação
- **Análise Exploratória**: Gráficos e estatísticas detalhadas das infrações
- **Exportação de Dados**: Download em Excel (formato Anexo J) e CSV
- **Ranking de Acurácia**: Análise das divergências entre os níveis de confiança

## Níveis de Acurácia

O sistema classifica as infrações em três níveis de acurácia, baseados no consenso de múltiplas IAs:

| Nível | Descrição | Taxa de Erro Esperada |
|-------|-----------|----------------------|
| **ALTA** (Verde) | Consenso das 3 IAs | 1-2% |
| **MÉDIA** (Amarelo) | Maioria 2x1 | Até 5% |
| **BAIXA** (Vermelho) | IAs divergentes | Requer avaliação manual |

## Requisitos

### Dependências Python

```txt
streamlit
pandas
numpy
plotly
sqlalchemy
openpyxl
smbclient (opcional - para salvar na rede)
```

### Instalação

```bash
pip install streamlit pandas numpy plotly sqlalchemy openpyxl
```

Para suporte a salvamento em rede:
```bash
pip install smbclient
```

## Configuração

### Credenciais do Banco de Dados

Crie o arquivo `.streamlit/secrets.toml` com as credenciais de acesso ao Impala:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

### Variáveis de Ambiente

O sistema se conecta ao banco de dados Impala com as seguintes configurações:

- **Host**: `bdaworkernode02.sef.sc.gov.br`
- **Porta**: `21050`
- **Database**: `niat`
- **Autenticação**: LDAP com SSL

## Execução

```bash
streamlit run "GESSUPER (3).py"
```

## Estrutura do Sistema

### Módulos Principais

| Módulo | Descrição |
|--------|-----------|
| **Consulta por Empresa** | Busca infrações por CNPJ ou Inscrição Estadual |
| **Ranking** | Top 100 empresas por valor de infrações |
| **Ranking Acurácia** | Análise de divergências entre níveis |
| **Pesquisa de Produtos** | Busca produtos por descrição |
| **Análise Exploratória** | Gráficos e estatísticas detalhadas |

### Fontes de Dados

O sistema consulta as seguintes tabelas no banco de dados `niat`:

- `infracoes_gessuper_nfce_3M` - Infrações de NFC-e
- `infracoes_gessuper_cupons_3M` - Infrações de Cupons Fiscais (ECF)
- `tabela_ncm` - Nomenclatura Comum do Mercosul
- `tabela_cfop` - Códigos Fiscais de Operação
- `usr_sat_ods.vw_ods_contrib` - Cadastro de contribuintes

## Exportação de Dados

### Formato Excel (Anexo J)

O sistema gera arquivos Excel com duas abas:

1. **ANEXO J1 - NOTAS DE SAÍDAS**: Dados detalhados com:
   - Informações dos documentos fiscais (NFC-e/Cupom)
   - Campos calculados pelo Fisco (legislação, alíquota correta, ICMS devido)
   - Fórmulas para recálculo automático
   - Link direto para DANFE (quando disponível)

2. **ANEXO J2 - ICMS DEVIDO**: Resumo mensal com:
   - ICMS destacado por período
   - ICMS apurado (calculado)
   - ICMS não recolhido

### Limites de Exportação

- **Máximo por arquivo Excel**: 1.000.000 linhas
- **Aviso de arquivo grande**: 200.000 linhas (recomenda CSV)
- **Arquivos grandes**: Divididos automaticamente em partes (ZIP)

### Formato CSV

- Separador: ponto e vírgula (;)
- Encoding: Latin-1 (ANSI)
- Decimal: vírgula (,)

## Cache e Performance

| Tipo de Cache | Duração |
|---------------|---------|
| Consultas de empresas | 30 minutos |
| Ranking | 24 horas |
| Tabelas de referência (NCM/CFOP) | 24 horas |
| Timeout de sessão inativa | 30 minutos |

## Funcionalidades Detalhadas

### Análise Exploratória

Disponível após consultar uma empresa:

- **KPIs Principais**: Total de infrações, quantidade de itens, média por item
- **Evolução Temporal**: Gráfico de linha por período
- **Distribuição por CFOP**: Análise das operações fiscais
- **Top 10 NCMs**: Produtos com maior valor de infração
- **Distribuição por Alíquota**: Análise das alíquotas aplicadas
- **Comparativo de Níveis**: Diferença entre BAIXA, MÉDIA e ALTA

### Comparativo de Níveis

Mostra a diferença nos valores de infrações entre os três níveis de acurácia, permitindo identificar onde há maior divergência entre as IAs.

### Pesquisa de Produtos

Permite buscar produtos por descrição para:
- Verificar como estão sendo tributados
- Identificar discrepâncias de alíquotas
- Analisar padrões de tributação por NCM/CFOP

## Segurança

- Conexão SSL com o banco de dados
- Credenciais armazenadas em arquivo secrets separado
- Limpeza automática de cache e dados em sessões inativas

## Autores

Desenvolvido pela equipe **NIAT** (Núcleo de Inteligência e Análise Tributária) da **Secretaria de Estado da Fazenda de Santa Catarina**.

## Licença

Uso interno da Receita Estadual de Santa Catarina.
