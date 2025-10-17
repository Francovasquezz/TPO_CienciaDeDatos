# scripts/r_tm_latest_values_batch.R
# Uso:
#   Rscript scripts/r_tm_latest_values_batch.R "Argentina" 2024 data/processed/tm_values_ARG_2024_latest.csv "https://www.transfermarkt.com/primera-division/startseite/wettbewerb/AR1N"

suppressPackageStartupMessages({
  library(worldfootballR)
  library(dplyr)
  library(readr)
  library(rlang)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Uso: Rscript scripts/r_tm_latest_values_batch.R <COUNTRY_NAME> <START_YEAR> <OUT_CSV> [LEAGUE_URL]")
}
country    <- args[[1]]
start_year <- as.integer(args[[2]])
out_csv    <- args[[3]]
league_url <- if (length(args) >= 4) args[[4]] else NA

dir.create(dirname(out_csv), recursive = TRUE, showWarnings = FALSE)

message(sprintf(">> worldfootballR: country=%s, start_year=%s, league_url=%s", country, start_year, ifelse(is.na(league_url),"NA",league_url)))

df <- tryCatch({
  if (is.na(league_url) || league_url == "" || league_url == "NA") {
    tm_player_market_values(country_name = country, start_year = start_year)
  } else {
    tm_player_market_values(country_name = NA, start_year = start_year, league_url = league_url)
  }
}, error = function(e) {
  message("ERROR en tm_player_market_values: ", conditionMessage(e))
  return(data.frame())
})

message(sprintf(">> Filas crudas: %s", nrow(df)))

if (nrow(df) == 0) {
  # Guardar un CSV vacío con encabezados para debug
  out <- tibble(player_name=character(), club_name=character(), market_value_eur=integer())
  write_csv(out, out_csv)
  message(">> Sin filas. CSV vacío escrito (encabezados).")
  quit(save="no")
}

# elegir columna de fecha disponible si existe
order_col <- intersect(c("date","last_update","update_time","last_update_date"), names(df))
order_col <- if (length(order_col) > 0) order_col[[1]] else NA_character_

out <- df %>%
  { if (!is.na(order_col)) arrange(., .data$player, desc(!!sym(order_col))) else arrange(., .data$player) } %>%
  distinct(.data$player, .keep_all = TRUE) %>%
  transmute(
    player_name      = .data$player,
    club_name        = .data$team,
    market_value_eur = .data$market_value_in_eur
  )

write_csv(out, out_csv)
message(sprintf(">> Filas finales: %s → %s", nrow(out), out_csv))
