# -------------------------------------------------------------------
# ANÁLISE DE SÉRIE TEMPORAL - INCIDÊNCIA DE HEPATITE A E DOADORES
# 
# OBJETIVO:
# 1. Carregar e consolidar dados de notificação de Hepatite (SINAN).
# 2. Filtrar casos de Hepatite A na faixa etária de 1 a 4 anos.
# 3. Carregar dados de população (IBGE/Datasus) e de doadores (outra fonte).
# 4. Unir as bases de dados em uma série temporal mensal completa.
# 5. Calcular taxas de incidência por 100.000 habitantes.
# 
# -------------------------------------------------------------------


# --- 0. CONFIGURAÇÃO INICIAL ---

# Limpar o ambiente (opcional, bom para testes, mas cuidado em scripts)
# rm(list = ls())

# Carregar todas as bibliotecas no início
library(read.dbc) # Para ler arquivos .dbc do DATASUS
library(dplyr)    # Para manipulação de dados (verbos como filter, mutate, etc.)
library(stringr)  # Para manipulação de strings (textos)
library(lubridate)# Para manipulação de datas
library(purrr)    # Para programação funcional (usado aqui com map)
library(tidyr)    # Para organizar dados (usado aqui com replace_na)

# NOTA SOBRE O DIRETÓRIO DE TRABALHO:
# É uma má prática usar setwd() em scripts compartilháveis (GitHub).
# Recomendo usar RStudio Projects. O script abaixo assume que os
# arquivos .dbc e .csv estão na raiz do projeto ou em uma subpasta.
# 
# Se você *não* usa projetos, descomente a linha abaixo e ajuste o caminho:
# setwd("/home/ramos/Documentos/R/Controle sintetico e its/")


# --- 1. FUNÇÕES AUXILIARES ---

#' @title Leitor seguro de arquivos .dbc
#' @description Tenta ler um arquivo .dbc. Se falhar, retorna NULL e avisa.
#' @param arq Caminho do arquivo .dbc
#' @return Um data.frame com os dados ou NULL se houver erro.
ler_dbc_seguro <- function(arq) {
  message("Lendo: ", arq)
  out <- tryCatch(
    read.dbc(arq),
    error = function(e) {
      message("  Falha ao ler: ", arq, " - Erro: ", e$message)
      NULL
    }
  )
  # Adiciona uma coluna para saber a origem do dado
  if (!is.null(out)) {
    out$arquivo_origem <- basename(arq)
  }
  out
}

#' @title Decodifica Idade Padrão SUS (NU_IDADE_N)
#' @description Converte o formato de 4 dígitos (ex: "4025" = 25 anos) em idade decimal em anos.
#' @param x Vetor (numérico ou string) com as idades no formato SUS.
#' @return Um vetor numérico com a idade em anos (decimal para meses/dias).
decodifica_idade_sus <- function(x) {
  # Garante 4 dígitos, preenchendo com zero à esquerda
  x_chr <- sprintf("%04d", as.integer(as.character(x)))
  
  # 1º dígito = unidade (1=hora, 2=dia, 3=mês, 4=ano)
  unidade <- substr(x_chr, 1, 1)
  # 3 últimos dígitos = valor
  valor   <- as.integer(substr(x_chr, 2, 4))
  
  idade_anos <- dplyr::case_when(
    unidade == "4" ~ valor * 1,            # Anos
    unidade == "3" ~ valor / 12,           # Meses -> anos
    unidade == "2" ~ valor / 365.25,       # Dias  -> anos
    unidade == "1" ~ valor / (24 * 365.25), # Horas -> anos
    TRUE ~ NA_real_
  )
  
  idade_anos
}

#' @title Cálculo Seguro de Taxa por 100 mil
#' @description Calcula (numerador / denominador) * 100.000, com tratamento seguro para NAs e divisão por zero.
#' @param num Vetor do numerador (ex: casos)
#' @param den Vetor do denominador (ex: população)
#' @param scale Fator de escala (padrão 100000)
#' @return A taxa calculada.
safe_rate <- function(num, den, scale = 100000) {
  ifelse(is.na(den), NA_real_,
         ifelse(den == 0,
                # Se denominador é 0, taxa é 0 se numerador for 0/NA, senão é NA (indefinido)
                ifelse(is.na(num) | num == 0, 0, NA_real_),
                # Cálculo padrão
                (ifelse(is.na(num), 0, num) / den) * scale
         )
  )
}


# --- 2. CARGA E PREPARAÇÃO DOS DADOS DE HEPATITE (SINAN) ---

# 2.1. Ler e consolidar arquivos .dbc
# Lista todos os arquivos que começam com "HEPA" (maiúsculo ou minúsculo)
arquivos_dbc <- list.files(
  path = ".", # Assume que estão no diretório atual
  pattern = "^HEPA.*\\.(dbc|DBC)$",
  full.names = TRUE # Importante para a função de leitura
)

message("Encontrados ", length(arquivos_dbc), " arquivos DBC de Hepatite.")
print(basename(arquivos_dbc))

# Lê todos os arquivos, empilhando-os em um único data.frame
dados_raw <- arquivos_dbc |>
  map(ler_dbc_seguro) |>
  bind_rows()

message("Total de ", nrow(dados_raw), " registros lidos.")

# 2.2. Limpeza e Transformação dos dados brutos
dados_limpos <- dados_raw |>
  mutate(
    # Converte colunas de classificação e marcadores para inteiros
    CLASSI_FIN = suppressWarnings(as.integer(as.character(CLASSI_FIN))),
    HAV        = suppressWarnings(as.integer(as.character(HAV))),
    ANTIHAVIGM = suppressWarnings(as.integer(as.character(ANTIHAVIGM))),
    
    # Garante 'FORMA' como string de 2 dígitos (ex: "01", "04", "99")
    FORMA = str_pad(as.character(FORMA), width = 2, side = "left", pad = "0"),
    
    # Converte datas (lidando com formatos inconsistentes)
    DT_DIAG    = suppressWarnings(lubridate::ymd(DT_DIAG)),
    DT_NOTIFIC = suppressWarnings(lubridate::ymd(DT_NOTIFIC)),
    
    # Cria data do evento (prioriza DT_DIAG, senão usa DT_NOTIFIC)
    data_evento = coalesce(DT_DIAG, DT_NOTIFIC),
    
    # Cria colunas de data/tempo para agregação
    # Arredonda para o 1º dia do mês (útil para joins)
    data_mes = floor_date(data_evento, "month"), 
    ano      = year(data_mes),
    mes      = month(data_mes),
    
    # Decodifica a idade para anos
    idade_anos = decodifica_idade_sus(NU_IDADE_N)
  )

# 2.3. Filtrar casos de interesse (Hepatite A, 1-4 anos)
hepa_1_4 <- dados_limpos |>
  filter(
    HAV == 1, # Filtra apenas casos de Hepatite A
    idade_anos >= 1 & idade_anos < 5 # Filtra faixa etária (1 ano completo até 4.99 anos)
  )

# 2.4. Agregar casos por mês
hepa_1_4_mensal <- hepa_1_4 |>
  # Garante que não estamos contando registros sem data
  filter(!is.na(data_mes)) |> 
  count(data_mes, ano, mes, name = "casos_hepaA") |>
  arrange(data_mes)

# Salva um CSV intermediário (opcional)
write.csv(hepa_1_4_mensal, "casos_hepatite_A_1a4_mensal.csv", row.names = FALSE)


# --- 3. CARGA DE DADOS EXTERNOS (POPULAÇÃO E DOADORES) ---

# 3.1. População
# Assume um CSV com colunas: 'ano', 'Pop.geral', 'Pop.1.a.4'
pop <- read.csv("Populacao.csv")
pop <- na.omit(pop) # Remove linhas com qualquer NA (cuidado se houver NAs parciais)

# 3.2. Doadores (e outras variáveis de saúde)
# Assume um CSV com colunas: 'data' (formato YYYY-MM-DD) e contagens
doadores <- read.csv("Doadores.csv")

# Verifica nomes das colunas (bom para debugar)
message("Colunas HepA mensal:")
print(colnames(hepa_1_4_mensal))
message("Colunas População:")
print(colnames(pop))
message("Colunas Doadores:")
print(colnames(doadores))


# --- 4. INTEGRAÇÃO DOS DADOS (JOIN) ---

# O objetivo é criar uma base de dados "scaffold" (esqueleto)
# que tenha todos os meses da série (vindos de 'doadores')
# e juntar as outras informações a ela.

# 4.1. Preparar base de doadores (garantir datas mensais)
doadores_prep <- doadores |>
  mutate(
    data = as.Date(data),
    # Garante que a data é o 1º dia do mês para o join
    data_mes = floor_date(data, "month") 
  ) |>
  select(-data) # Remove a data original para evitar confusão

# 4.2. Criar o "esqueleto" de meses
base_meses <- doadores_prep |>
  distinct(data_mes) |>
  filter(!is.na(data_mes)) |>
  mutate(
    ano = year(data_mes),
    mes = month(data_mes)
  ) |>
  arrange(data_mes)

# 4.3. Juntar População (baseada no ano)
base_pop <- base_meses |>
  left_join(pop, by = "ano")

# 4.4. Juntar Casos de Hepatite A (baseado no mês/ano)
# Usamos left_join para manter todos os meses, mesmo os sem casos
base_pop_hepa <- base_pop |>
  left_join(hepa_1_4_mensal, by = c("data_mes", "ano", "mes")) |>
  mutate(
    # Onde não houve join (meses sem casos), preenche com 0
    casos_hepaA = replace_na(casos_hepaA, 0)
  )

# 4.5. Juntar dados dos Doadores
base_final <- base_pop_hepa |>
  left_join(doadores_prep, by = "data_mes")


# --- 5. CÁLCULO DAS TAXAS DE INCIDÊNCIA ---

# 5.1. Substituir NAs por 0 nos numeradores (contagens de eventos)
# É seguro assumir que NA em contagem de evento = 0 eventos.
# NÃO fazemos isso para População (denominadores), pois NA lá é "dado faltante".
base_final_zeros <- base_final |>
  mutate(
    across(
      c(LER_ADULTO_long, LEISHMANIOSE_VISCERAL_ADULTO_long,
        IAM, AVC, HIV, Obito, `nascidos.vivos`,
        INTOXICACAO_EXONENA_1A4_long, LESH_TEG_1A4_long, Meningite_1A4),
      ~ replace_na(., 0)
    )
  )

# 5.2. Calcular taxas por 100.000 habitantes
taxas_100k <- base_final_zeros |>
  mutate(
    # Taxas usando População Geral
    taxa_LER_ADULTO        = safe_rate(LER_ADULTO_long, Pop.geral),
    taxa_LEISH_VIS_ADULTO  = safe_rate(LEISHMANIOSE_VISCERAL_ADULTO_long, Pop.geral),
    taxa_IAM               = safe_rate(IAM, Pop.geral),
    taxa_AVC               = safe_rate(AVC, Pop.geral),
    taxa_HIV               = safe_rate(HIV, Pop.geral),
    taxa_Obito             = safe_rate(Obito, Pop.geral),
    taxa_nascidos_vivos    = safe_rate(`nascidos.vivos`, Pop.geral),
    
    # Taxas usando População 1-4 anos
    taxa_INTX_1A4          = safe_rate(INTOXICACAO_EXONENA_1A4_long, Pop.1.a.4),
    taxa_LESH_TEG_1A4      = safe_rate(LESH_TEG_1A4_long, Pop.1.a.4),
    taxa_Meningite_1A4     = safe_rate(Meningite_1A4, Pop.1.a.4),
    taxa_HepatiteA_1A4     = safe_rate(casos_hepaA, Pop.1.a.4)
  ) |>
  # Seleciona apenas as colunas finais de interesse
  select(
    data = data_mes, # Renomeia para clareza
    taxa_LER_ADULTO,
    taxa_LEISH_VIS_ADULTO,
    taxa_IAM,
    taxa_AVC,
    taxa_HIV,
    taxa_Obito,
    taxa_nascidos_vivos,
    taxa_INTX_1A4,
    taxa_LESH_TEG_1A4,
    taxa_Meningite_1A4,
    taxa_HepatiteA_1A4
  )


# --- 6. SALVAR RESULTADO FINAL ---

# Exibe os primeiros resultados no console
message("Resultado final (taxas por 100k):")
print(head(taxas_100k))

# Salva o arquivo CSV final
write.csv(taxas_100k, "taxas_doadores_e_hepatite_100k.csv", row.names = FALSE)

message("Script concluído. Arquivo final salvo em 'taxas_doadores_e_hepatite_100k.csv'")