===================================================================
 # SCRIPT DE CARGA E CONSOLIDAÇÃO DE DADOS DE SAÚDE
#Objetivo: Unir múltiplas fontes de dados (DBC e CSV) em uma
#única tabela de série temporal por data.
===================================================================
 # Limpar o ambiente
rm(list = ls())

#1. CARREGAR PACOTES ---
  (Instale com: install.packages(c("dplyr", "lubridate", ...)))
library(dplyr)
library(lubridate)
library(tidyr)
library(read.dbc)
library(data.table) # Para fread()
library(tools) # Para file_path_sans_ext()
library(janitor) # Para clean_names()
library(stringr) # Para manipulação de texto
library(purrr) # Para a função reduce()

# 2. DEFINIR FUNÇÕES AUXILIARES ---
  #' Lê todos os arquivos .dbc de uma pasta e os combina em um único dataframe.
  #' @param dirpath Caminho para a pasta contendo os arquivos .dbc.
  #' @return Um dataframe com todas as linhas dos arquivos .dbc combinadas.
  ler_dbc_pasta <- function(dirpath) {
    arqs <- list.files(dirpath, pattern = "\.dbc$", full.names = TRUE)
    if (length(arqs) == 0) {
      warning("Nenhum arquivo .dbc encontrado em: ", dirpath)
      return(data.frame())
    }
    arqs |>
      lapply(read.dbc::read.dbc) |>
      dplyr::bind_rows()
  }

#' Sumariza um dataframe de notificações por mês/ano.
#' Tenta detectar automaticamente a coluna de data (DT_DIAG, COMP, ou ANO+MES).
#' @param df Dataframe de entrada.
#' @return Um dataframe sumarizado com colunas ano_mes, n_registros, ano, mes.
sumarizar_por_mes_ano <- function(df) {
  if ("DT_DIAG" %in% names(df)) {
    df <- df |> dplyr::mutate(DT_DIAG = lubridate::ymd(DT_DIAG))
    base <- df |>
      dplyr::filter(!is.na(DT_DIAG)) |>
      dplyr::mutate(ano_mes = lubridate::floor_date(DT_DIAG, "month"))
  } else if ("COMP" %in% names(df)) {
    base <- df |>
      dplyr::mutate(ano_mes = lubridate::ymd(paste0(substr(COMP, 1, 4), "-", substr(COMP, 5, 6), "-01")))
  } else if (all(c("ANO", "MES") %in% names(df))) {
    base <- df |> dplyr::mutate(ano_mes = lubridate::ymd(paste0(ANO, "-", sprintf("%02d", as.integer(MES)), "-01")))
  } else {
    stop("Nenhuma coluna de data/competência reconhecida (DT_DIAG, COMP, ou ANO+MES).")
  }
  
  ini <- lubridate::floor_date(min(base$ano_mes, na.rm = TRUE), "month")
  fim <- lubridate::floor_date(max(base$ano_mes, na.rm = TRUE), "month")
  
  base |>
    dplyr::count(ano_mes, name = "n_registros") |>
    tidyr::complete(
      ano_mes = seq(ini, fim, by = "month"),
      fill = list(n_registros = 0)
    ) |>
    dplyr::mutate(
      ano = lubridate::year(ano_mes),
      mes = lubridate::month(ano_mes)
    ) |>
    dplyr::arrange(ano_mes)
}

#' Transforma dataframes 'wide' (com meses como colunas) para 'long'.
#' @param df Dataframe wide (ex: colunas 'Ano', 'Jan', 'Fev', ...).
#' @param indicador Nome do indicador (usado apenas para referência interna).
#' @return Um dataframe long com colunas (indicador, data, ano, mes, valor).
to_long_mensal <- function(df, indicador = deparse(substitute(df))) {
  df2 <- df |> janitor::clean_names()
  
  col_ano <- names(df2)[tolower(names(df2)) %in% c("ano")]
  if (length(col_ano) == 0) stop("Não encontrei a coluna 'Ano'.")
  
  meses_abrev <- c("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")
  cols_meses <- intersect(meses_abrev, names(df2))
  
  df_long <- df2 |>
    pivot_longer(
      cols = all_of(cols_meses),
      names_to = "mes_abrev",
      values_to = "valor"
    ) |>
    mutate(
      mes = match(mes_abrev, meses_abrev),
      ano = .data[[col_ano]],
      data = ymd(sprintf("%04d-%02d-01", as.integer(ano), mes)),
      indicador = indicador,
      # Converte 'valor' para numérico, tratando vírgula como decimal
      valor = as.numeric(str_replace(valor, ",", "."))
    ) |>
    select(indicador, data, ano, mes, valor) |>
    arrange(data) |>
    # Remove linhas onde a data não pôde ser criada (ex: linhas "Total" sem ano)
    filter(!is.na(data))
  
  df_long
}

# 3. CARREGAR DADOS .DBC (HIV) ---
 # 3.1. Processar HIV (com AIDS)
caminho_hiv <- "~/Documentos/R/Controle sintetico e its/CONTROLES/HIV"
dados_hiv <- ler_dbc_pasta(caminho_hiv)

HIV_mes_ano <- dados_hiv |>
  dplyr::mutate(DT_DIAG = lubridate::ymd(DT_DIAG)) |>
  dplyr::filter(!is.na(DT_DIAG)) |>
  dplyr::mutate(
    ano = lubridate::year(DT_DIAG),
    mes = lubridate::month(DT_DIAG),
    ano_mes = lubridate::floor_date(DT_DIAG, "month")
  ) |>
  dplyr::count(ano, mes, ano_mes, name = "HIV") |>
  dplyr::arrange(ano, mes)

#3.2. Processar HIV (sem AIDS)
pasta_sem_aids <- "~/Documentos/R/Controle sintetico e its/CONTROLES/HIV_sem_aids"
dados_sem_aids <- ler_dbc_pasta(pasta_sem_aids)
resumo_sem_aids <- sumarizar_por_mes_ano(dados_sem_aids)

(Opcional) Salvar resumo:
  write.csv(resumo_sem_aids, "resumo_HIV_sem_aids_mensal.csv", row.names = FALSE)
#4. CARREGAR DADOS .CSV (Outros) ---
  Carrega todos os CSVs da pasta raiz 'CONTROLES' e os nomeia
automaticamente no ambiente (ex: LER.ADULTO, AVC, IAM, ...)
pasta_csv <- "~/Documentos/R/Controle sintetico e its/CONTROLES"
arquivos_csv <- list.files(pasta_csv, pattern = "\.csv$", full.names = TRUE, recursive = FALSE)

for (arq in arquivos_csv) {
  nome <- file_path_sans_ext(basename(arq))
  nome_obj <- make.names(nome)
  assign(
    nome_obj,
    data.table::fread(arq, encoding = "UTF-8", sep = "auto", dec = "auto",
                      na.strings = c("", "NA", "NaN", "-")) # Adicionado "-" como NA
  )
  message("✅ Carregado: ", nome_obj)
}

# 5. TRANSFORMAÇÃO E PREPARAÇÃO (ETL) ---
 # Preparar cada dataframe para o join, deixando apenas [data, valor_coluna]
#e garantindo que a coluna 'data' seja do tipo Date.
#5.1. Preparar dados de HIV (já processados do DBC)
df_hiv <- HIV_mes_ano |>
  select(data = ano_mes, HIV) |>
  filter(!is.na(data))

#5.2. Preparar dados 'Wide' (formatos Jan..Dez)
#A função 'to_long_mensal' já cria a coluna 'data' corretamente.
df_ler <- to_long_mensal(LER.ADULTO) |>
  select(data, LER_ADULTO = valor)

df_leish_v <- to_long_mensal(LEISHMANIOSE.VISCERAL.ADULTO) |>
  select(data, LEISHMANIOSE_VISCERAL_ADULTO = valor)

df_intox <- to_long_mensal(intoxicacao.exonena.de.1.a.4.anos) |>
  select(data, INTOXICACAO_EXONENA_1A4 = valor)

df_lesh_t <- to_long_mensal(LESH.TEG..1.A.ANOS) |>
  select(data, LESH_TEG_1A4 = valor)

df_meningite <- to_long_mensal(Meningite.1.a.4.anos) |>
  select(data, Meningite_1A4 = valor)

#5.3. Preparar dados 'Long' (formatos já mensais, carregados do CSV)
#Aqui precisamos converter a 'data' (que o fread pode ter lido como texto)
#e filtrar linhas de 'Total' (que têm 'data' como NA).
df_iam <- IAM |>
  mutate(data = as.Date(data)) |>
  filter(!is.na(data)) |>
  select(data, IAM)

df_avc <- AVC |>
  mutate(data = as.Date(data)) |>
  filter(!is.na(data)) |>
  select(data, AVC = internacoes) # Renomeia a coluna de valor

df_obitos <- obitos |>
  mutate(data = as.Date(data)) |>
  filter(!is.na(data)) |>
  select(data, Obito)

df_nascidos <- nascidos.vivos |>
  mutate(data = as.Date(data)) |>
  filter(!is.na(data)) |>
  select(data, nascidos_vivos = nascidos vivos) # Renomeia para nome válido

#--- 6. UNIR TODOS OS DADOS ---
  Criar uma lista de todos os dataframes prontos para o join
lista_dfs_prontos <- list(
  df_hiv,
  df_ler,
  df_leish_v,
  df_intox,
  df_lesh_t,
  df_meningite,
  df_iam,
  df_avc,
  df_obitos,
  df_nascidos
)

#Usar purrr::reduce para aplicar full_join sucessivamente
dados_concatenados <- lista_dfs_prontos |>
  purrr::reduce(full_join, by = "data") |>
  arrange(data)

#--- 7. VERIFICAR RESULTADO ---
  message("✅ Concatenação finalizada.")
glimpse(dados_concatenados)
View(dados_concatenados)

#(Opcional) Salvar o resultado final
write.csv(dados_concatenados, "dados_concatenados_final.csv", row.names = FALSE)
saveRDS(dados_concatenados, "dados_concatenados_final.rds")