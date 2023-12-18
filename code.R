
install.packages("prettymapr")
remotes::install_github("ccao-data/ccao")


# ---- Setup -----
library(remotes); library(tidyr); library(glue) # Data manipulation
library(dplyr); library(sf)
library(RSQLite); library(DBI)
library(ptaxsim); library(assessr); library(ccao) # CCAO's packages
library(here) # Directories
library(ggplot2); library(ggspatial); library(prettymapr)

# Create the DB connection with the default name expected by PTAXSIM functions
ptaxsim_db_conn <- DBI::dbConnect(RSQLite::SQLite(), here::here("PTAXSIM DB", "ptaxsim.db"))

# Based on Cook Viewer, these are the four PINs that cover the raceway
raceway <- c("02252020080000", "02243030070000", "02251000050000", "02262010100000")

# Just to orient ourselves to the contents of the database
tables <- DBI::dbGetQuery(
  ptaxsim_db_conn,
  "select * from sqlite_master"
  , .con = ptaxsim_db_conn
)

# Let's only look at the local taxing agencies and exclude the big ones
agencies <- ptaxsim::tax_bill(year_vec = 2021, pin_vec = raceway) %>%
  dplyr::distinct(agency_num, agency_name, agency_major_type) %>%
  dplyr::filter(agency_num %in% c("030020000", "030020001", "040030000", "042130000", "043060000", "050950000"))
# For use in SQL queries
agencies <- paste0(unlist(agencies$agency_num), collapse="','")
agencies <- glue_sql_collapse(agencies, sep = ", ")

# ---- Ingest ----
# Get the PINs that share a taxing agency with the raceway
# and get their AVs and bills
# and calculate their EAVs
pins <- DBI::dbGetQuery(
  ptaxsim_db_conn,
  glue_sql("
  WITH tax_codes AS (
  SELECT agency_num, tax_code_num 
  FROM tax_code 
  WHERE year = 2021
  AND tax_code.agency_num IN ('{agencies}')
  ),
  pin_sub AS (
  SELECT pin.year, pin.pin, pin.av_certified, pin.class, pin.tax_code_num
   , (exe_homeowner + exe_senior + exe_freeze + exe_longtime_homeowner + 
   exe_disabled + exe_vet_returning + exe_vet_dis_lt50 +      
   exe_vet_dis_50_69 + exe_vet_dis_ge70 + exe_abate) AS exempt_eav
   , av_certified * eq_factor.eq_factor_final AS eav_certified  
   FROM pin
   INNER JOIN eq_factor
      ON eq_factor.year = pin.year 
   WHERE pin.year = 2021
  )
  SELECT pin_sub.*
  , agency_info.agency_name, agency_info.agency_num 
    FROM pin_sub
    INNER JOIN tax_codes
      ON pin_sub.tax_code_num = tax_codes.tax_code_num
     INNER JOIN agency_info
      ON tax_codes.agency_num = agency_info.agency_num
    ")
) %>%
  dplyr::mutate(
    pin10 = substr(pin, 1, 10),
    major_class = substr(class, 1, 1),
    major_class_fct = recode_factor(
      major_class,
      "0" = "0 & 1 - Vacant and Exempt",
      "1" = "0 & 1 - Vacant and Exempt",
      "2" = "2 - Residential",
      "3" = "3 & 5 - Commercial",
      "5" = "3 & 5 - Commercial",
      "6" = "6, 7, 8 - Incentive",
      "7" = "6, 7, 8 - Incentive",
      "8" = "6, 7, 8 - Incentive"
    )
  )

# 2021 raceway value
raceway_value <- pins %>%
  dplyr::filter(pin %in% raceway) %>%
  distinct(pin, eav_certified)

sum(raceway_value$eav_certified)

# Find median values
medians <- pins %>%
  dplyr::group_by(year, agency_name, agency_num, major_class_fct) %>%
  dplyr::summarise(
    median_av = median(av_certified)
    , pin_count = n()
  ) 

test <- DBI::dbGetQuery(
  ptaxsim_db_conn,
  glue_sql("
SELECT * FROM eq_factor
WHERE year = 2021
LIMIT 10
"))
  
test2 <- pins %>%
  dplyr::filter( pin == '02012020020000')

pins %>%
  dplyr::filter(av_certified == 21670)