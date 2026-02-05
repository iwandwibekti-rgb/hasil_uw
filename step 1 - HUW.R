
library(tidyverse)
library(dtplyr)
library(data.table)
library(writexl)
library(readxl)
library(scales)
library(hablar)
options(scipen = 999)

###############################################################
setwd("D:/RIDWAN/9. RISK PROFILE")
data_rp <- readRDS("data_rp_20260130.rds")
########################################################

# Function to calculate policy days in each calendar year with valuation date split
calculate_calendar_days <- function(eff_date, exp_date, valuation_date,
                                    start_year = 2022, end_year = 2030) {
  
  # Initialize result vector
  result <- list()
  
  # Extract valuation year
  val_year <- year(valuation_date)
  
  for (year in start_year:end_year) {
    year_start <- as.Date(paste0(year, "-01-01"))
    year_end <- as.Date(paste0(year, "-12-31"))
    
    # Check if this year matches valuation year (vectorized)
    is_val_year <- year == val_year
    
    # Initialize days columns
    days_1 <- rep(0, length(eff_date))
    days_2 <- rep(0, length(eff_date))
    days_regular <- rep(0, length(eff_date))
    
    if (any(is_val_year)) {
      # For rows where this is the valuation year, split into two periods
      val_rows <- which(is_val_year)
      
      # Period 1: Year start to valuation date
      overlap_start_1 <- pmax(eff_date[val_rows], year_start)
      overlap_end_1 <- pmin(exp_date[val_rows], valuation_date[val_rows])
      days_1[val_rows] <- pmax(0, as.numeric(overlap_end_1 - overlap_start_1 + 1))
      
      # Period 2: Valuation date + 1 to year end
      val_date_plus_1 <- valuation_date[val_rows] + 1
      overlap_start_2 <- pmax(eff_date[val_rows], val_date_plus_1)
      overlap_end_2 <- pmin(exp_date[val_rows], year_end)
      days_2[val_rows] <- pmax(0, as.numeric(overlap_end_2 - overlap_start_2 + 1))
    }
    
    # For rows where this is NOT the valuation year, regular calculation
    non_val_rows <- which(!is_val_year)
    if (length(non_val_rows) > 0) {
      overlap_start <- pmax(eff_date[non_val_rows], year_start)
      overlap_end <- pmin(exp_date[non_val_rows], year_end)
      days_regular[non_val_rows] <- pmax(0, as.numeric(overlap_end - overlap_start + 1))
    }
    
    # Add columns based on whether any rows have this as valuation year
    if (any(is_val_year)) {
      result[[paste0("DAYS_", year, "_1")]] <- days_1
      result[[paste0("DAYS_", year, "_2")]] <- days_2
    } else {
      result[[paste0("DAYS_", year)]] <- days_regular
    }
  }
  
  return(as.data.frame(result))
}

###############################################################
# Updated data processing with dynamic calendar days

data_polis <- data_rp %>%
  mutate(
    AS_OF_DATE = as_date(AS_OF_DATE),
    EFF_DATE   = as_date(EFF_DATE),
    EXP_DATE   = as_date(EXP_DATE),
    SOB_GROUP  = if_else(MARKETING_NAME == "BlueBird Group", "BlueBird Group", ACCOUNT_NAME_PROXY),
    BB_PCKG    = if_else(MARKETING_NAME == "BlueBird Group", PCKG_DESC, NA_character_),
    IS_HE = case_when(
      COB == "EA" & str_detect(LOB, "CONSTRUCTION PLANT|HEAVY EQUIPMENT") ~ "CPM HE",
      COB == "EA" ~ "NON HE",
      TRUE ~ "other COB (non EA)"
    ),
    GROSS_CLAIM = coalesce(CLAIM_OS, 0) - coalesce(CLAIM_PAID, 0) - coalesce(CLAIM_SALVAGE, 0)- coalesce(CLAIM_VATX, 0),
    RI_CLAIM    = coalesce(CLAIM_OS_RI, 0) - coalesce(CLAIM_PAID_RI, 0) - coalesce(CLAIM_SALVAGE_RI, 0),
    RI_PREMIUM = coalesce(PREMIUM_FAC, 0) + coalesce(PREMIUM_TREATY, 0),
    RI_COMM = coalesce(COMM_FAC, 0) + coalesce(COMM_RI, 0)
  ) %>%
  mutate(
    SOB_GROUP    = toupper(trimws(SOB_GROUP)),
    INSURED_NAME = toupper(trimws(INSURED_NAME))
  ) %>%
  group_by(
    AS_OF_DATE, POLICY_NO, INSURED_NAME, ACCOUNT_NAME, ACCOUNT_NAME_PROXY, RECLASS_TOB,
    SOB_GROUP, COB, LOB, TOB, BRANCH, IS_FRONTING, IS_GROUP, IS_HE, BB_PCKG
  ) %>%
  summarise(
    MARKETING_GROUP = last(MARKETING_GROUP),
    EFF_DATE        = min(EFF_DATE, na.rm = TRUE),
    EXP_DATE        = max(EXP_DATE, na.rm = TRUE),
    PREMIUM         = sum(OUR_PREMIUM, na.rm = TRUE),
    DISKON          = sum(OUR_DISCOUNT, na.rm = TRUE),
    KOMISI          = sum(OUR_COMMISSION, na.rm = TRUE),
    RI_PREMIUM      = sum(RI_PREMIUM, na.rm = TRUE),
    RI_COMM         = sum(RI_COMM, na.rm = TRUE),
    GROSS_CLAIM     = sum(GROSS_CLAIM, na.rm = TRUE),
    RI_CLAIM        = sum(RI_CLAIM, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    RECLASS_TOB = case_when(
      MARKETING_GROUP == "Partnership" ~ "PARTNERSHIP",
      T ~ RECLASS_TOB
    ),
    NET_CLAIM         = GROSS_CLAIM - RI_CLAIM,
    UW_YEAR           = year(EFF_DATE),
    UW_MONTH          = month(EFF_DATE),
    POLICY_PERIOD     = as.numeric(EXP_DATE - EFF_DATE + 1),
    INCLUDES_LEAP_DAY = as_date("2024-02-29") >= EFF_DATE & as_date("2024-02-29") <= EXP_DATE,
    IS_LONG_SHORT = case_when(
      INCLUDES_LEAP_DAY & POLICY_PERIOD > 367 ~ "LT",
      INCLUDES_LEAP_DAY & POLICY_PERIOD <= 367 ~ "ST",
      !INCLUDES_LEAP_DAY & POLICY_PERIOD > 366 ~ "LT",
      TRUE ~ "ST"
    ),
    VALUATION_DATE = AS_OF_DATE
  ) %>%
  rename(PCKG_DESC = BB_PCKG) %>%
  filter(UW_YEAR >= 2022, EFF_DATE <= AS_OF_DATE, PREMIUM >= 1) %>%
  bind_cols(calculate_calendar_days(.$EFF_DATE, .$EXP_DATE, .$VALUATION_DATE)) %>%
  arrange(UW_YEAR, desc(PREMIUM)) %>%
  select(
    AS_OF_DATE, VALUATION_DATE, UW_YEAR, UW_MONTH, POLICY_NO, INSURED_NAME, ACCOUNT_NAME,
    ACCOUNT_NAME_PROXY, SOB_GROUP, RECLASS_TOB, COB, LOB, TOB, BRANCH, IS_FRONTING, IS_GROUP,
    IS_HE, MARKETING_GROUP, PCKG_DESC, EFF_DATE, EXP_DATE, POLICY_PERIOD, IS_LONG_SHORT,
    PREMIUM, DISKON, KOMISI, RI_PREMIUM, RI_COMM, GROSS_CLAIM, RI_CLAIM, NET_CLAIM,
    starts_with("DAYS_"), everything()
  ) %>%
  select(-INCLUDES_LEAP_DAY)

#205938
nrow(data_polis)
View(data_polis)

###############################################################
## DATA OVR

setwd("D:/RIDWAN/2.1 ANALISA PORTFOLIO All COB 5 UW Year Terakhir/2025/202507_2")

data_ovr <- read_excel("Profitability Analysis as at 20250731.xlsx",
                       sheet = "DATA",
                       col_types = "text")

data_ovr.0 <- data_ovr %>%
  mutate(
    SOB_GROUP       = if_else(MARKETING_NAME == "BlueBird Group", 
                              "BlueBird Group", 
                              ACCOUNT_NAME)
  )%>%
  hablar::convert(hablar::num(PREMIUM, DISC, GWP, COMM, RI_PREM, RI_COMM, GROSS_EP, COMM_EARNED, RI_EP,
              GROSS_CLAIM_TOTAL, RI_CLAIM_TOTAL, OVR_pct, OPEX_pct,OVR_IDR,OPEX_IDR, NEP, UW_YEAR))

data_ovr.1 <- data_ovr.0 %>%
  group_by(
    UW_YEAR,
    SOB_GROUP,
    COB,
    IS_GROUP,
    PCKG_DESC
  )%>%
  summarise(
    OVR_RATIO = sum(OVR_IDR, na.rm = TRUE)/ sum(GROSS_EP, na.rm = TRUE),
    .groups = "drop"
  )%>%
  arrange(desc(OVR_RATIO))%>%
  mutate(
    SOB_GROUP = toupper(trimws(SOB_GROUP)),
  ) %>%
  distinct(UW_YEAR, SOB_GROUP,COB, IS_GROUP, PCKG_DESC, .keep_all = TRUE)
##############################################################

# Join tanpa duplikasi baris
data_polis.1 <- data_polis %>%
  left_join(
    data_ovr.1,
    by = c("UW_YEAR","SOB_GROUP", "COB", "IS_GROUP", "PCKG_DESC")
  )

nrow(data_polis)
nrow(data_polis.1)
##############################################################
setwd("Z:/Actuary/Reserve/0. Produksi Polis Digital")
data.digital <- read_excel("Polis Digital Produksi dan UPR - 20260131.xlsx")

data.digital <- data.digital %>% 
  select(
    POLICY_NO,
    TANGGAL_MULAI,
    TANGGAL_BERAKHIR
  ) %>% 
  rename(
    EFF_DATE = TANGGAL_MULAI,
    EXP_DATE = TANGGAL_BERAKHIR
  ) %>% 
  mutate(
    EFF_DATE = as.Date(EFF_DATE),
    EXP_DATE = as.Date(EXP_DATE)
  )


data_polis.1 <- data_polis.1 %>%
  left_join(data.digital, by = "POLICY_NO", suffix = c("", ".new")) %>%
  mutate(
    EFF_DATE.new = as.Date(EFF_DATE.new),
    EXP_DATE.new = as.Date(EXP_DATE.new),
    EFF_DATE = coalesce(EFF_DATE.new, as.Date(EFF_DATE)),
    EXP_DATE = coalesce(EXP_DATE.new, as.Date(EXP_DATE))
  ) %>%
  select(-EFF_DATE.new, -EXP_DATE.new)

##############################################################

polis_xol <- c("10.03.01.24.06.0.00460",
               "10.03.01.24.06.0.00398",
               "10.03.01.24.08.0.00545",
               "10.03.02.24.03.0.00852",
               "10.03.02.24.08.0.00293",
               "29.03.02.24.07.0.00039",
               "28.03.02.24.09.0.00009",
               "10.03.02.24.08.0.00326",
               "28.03.02.24.08.0.00079")

##############################################################
cek_xol <- data_polis.1 %>%
  filter(POLICY_NO %in% polis_xol)%>%
  group_by(POLICY_NO, INSURED_NAME)%>%
  summarise(
    GROSS_CLAIM = sum(GROSS_CLAIM, na.rm = TRUE),
    RI_CLAIM    = sum(RI_CLAIM, na.rm = TRUE),
    NET_CLAIM   = sum(NET_CLAIM, na.rm = TRUE),
    .groups = "drop"
  )%>%
  mutate(
    NET_CLAIM_2 = case_when(
      POLICY_NO == "10.03.02.24.03.0.00852" ~ pmin(NET_CLAIM,350e6),
      POLICY_NO == "10.03.02.24.08.0.00293" ~ pmin(NET_CLAIM,350e6),
      POLICY_NO == "29.03.02.24.07.0.00039" ~ pmin(NET_CLAIM,350e6),
      POLICY_NO == "28.03.02.24.09.0.00009" ~ pmin(NET_CLAIM,350e6),
      POLICY_NO == "10.03.02.24.08.0.00326" ~ pmin(NET_CLAIM,350e6),
      POLICY_NO == "28.03.02.24.08.0.00079" ~ pmin(NET_CLAIM,350e6),
      
      POLICY_NO == "10.03.01.24.08.0.00545" ~ 0.05035627*3e9,
      POLICY_NO == "10.03.01.24.06.0.00460" ~ 0.5225347*3e9,
      POLICY_NO == "10.03.01.24.06.0.00398" ~ 0.4271091*3e9,
      TRUE ~ 0),
    XOL_AMT = GROSS_CLAIM - RI_CLAIM - NET_CLAIM_2
  )%>%
  select(
    POLICY_NO, XOL_AMT
  )
##############################################################


##### Input

RI_comm_Ratio    <- 0.985/100
OPEX_NonFronting <- 12/100
OPEX_Fronting    <- 0.1/100

####

data_polis.2 <- data_polis.1 %>%
  left_join(cek_xol , by= "POLICY_NO") %>%
  mutate(
    RI_COMM = case_when(
      IS_FRONTING == "FRONTING" & COB %in% c("LB", "MS") & UW_YEAR <= 2025 ~ RI_comm_Ratio * RI_PREMIUM,
      TRUE ~ RI_COMM)
  )%>%
  mutate(
    POLICY_PERIOD   = as.numeric(EXP_DATE - EFF_DATE + 1),
    EARNED_PERIOD   = as.numeric(difftime(AS_OF_DATE, EFF_DATE, units = "days"))+1,
    EARNED_PERIOD   = pmax(pmin(EARNED_PERIOD, POLICY_PERIOD), 0),
    UNEARNED_PERIOD = POLICY_PERIOD - EARNED_PERIOD,
    
    OVR_IDR         = coalesce(OVR_RATIO * (PREMIUM - DISKON),0),
    
    NWP             = PREMIUM - DISKON - KOMISI - OVR_IDR - RI_PREMIUM + RI_COMM,
    
    GWP_EARNED      = case_when(
      str_detect(str_to_upper(LOB), "CREDIT INSURANCE") ~ (PREMIUM - DISKON),
      TRUE ~ coalesce((EARNED_PERIOD / POLICY_PERIOD) * (PREMIUM - DISKON), 0)
    ),
    
    COMM_EARNED    = case_when(
      str_detect(str_to_upper(LOB), "CREDIT INSURANCE") ~ (KOMISI + OVR_IDR),
      TRUE ~ coalesce((EARNED_PERIOD / POLICY_PERIOD) * (KOMISI + OVR_IDR), 0)
    ),
    
    RI_EARNED       = coalesce((EARNED_PERIOD / POLICY_PERIOD) * (RI_PREMIUM - RI_COMM), 0),
    
    UPR             = coalesce(NWP - (GWP_EARNED - COMM_EARNED - RI_EARNED), 0),
    
    XOL_AMT         = coalesce(XOL_AMT,0),
    
    NET_CLAIM      = coalesce(GROSS_CLAIM - RI_CLAIM - XOL_AMT,0),
    
    OPEX_IDR = case_when(
      IS_FRONTING == "FRONTING"                         ~ OPEX_Fronting * GWP_EARNED,
      str_detect(str_to_upper(LOB), "CREDIT INSURANCE") ~ OPEX_Fronting * GWP_EARNED,
      IS_FRONTING == "NON_FRONTING"                     ~ OPEX_NonFronting * GWP_EARNED,
      TRUE ~ 0  
    )
  ) %>%
  relocate(XOL_AMT, .after = "RI_CLAIM") %>%
  relocate(UPR,     .after = "NWP")


sum(data_polis.2$GROSS_CLAIM)
sum(data_polis.2$RI_CLAIM)
sum(data_polis.2$XOL_AMT, na.rm = TRUE)
sum(data_polis.2$NET_CLAIM, na.rm = TRUE)


# Create simplified dataset data_polis.3
data_polis.3 <- data_polis.2 %>%
  group_by(AS_OF_DATE, UW_YEAR,UW_MONTH, INSURED_NAME, ACCOUNT_NAME, ACCOUNT_NAME_PROXY,
           RECLASS_TOB, SOB_GROUP, COB, LOB, TOB, BRANCH, MARKETING_GROUP,
           IS_FRONTING, IS_LONG_SHORT,IS_HE, OVR_RATIO) %>%
  summarise(
    PREMIUM     = sum(PREMIUM, na.rm = TRUE),
    DISKON      = sum(DISKON, na.rm = TRUE),
    KOMISI      = sum(KOMISI, na.rm = TRUE),
    OVR_IDR     = sum(OVR_IDR, na.rm = TRUE),
    RI_PREMIUM  = sum(RI_PREMIUM, na.rm = TRUE),
    RI_COMM     = sum(RI_COMM, na.rm = TRUE),
    NWP         = sum(NWP, na.rm = TRUE),
    UPR         = sum(UPR, na.rm = TRUE),
    
    GROSS_EP    = sum(GWP_EARNED, na.rm = TRUE),
    COMM_EARNED = sum(COMM_EARNED, na.rm = TRUE),
    RI_EP       = sum(RI_EARNED, na.rm = TRUE),
    
    
    OPEX_IDR    = sum(OPEX_IDR, na.rm = TRUE),
    GROSS_CLAIM = sum(GROSS_CLAIM, na.rm = TRUE),
    RI_CLAIM    = sum(RI_CLAIM, na.rm = TRUE),
    XOL_AMT     = sum(XOL_AMT, na.rm = TRUE),
    NET_CLAIM   = sum(NET_CLAIM, na.rm = TRUE),
    .groups = 'drop'
  )%>%
  mutate(
    HASIL_UW = coalesce(NWP - UPR - OPEX_IDR - NET_CLAIM,0)
  )

comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2025]))
comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2025 & data_polis.3$IS_FRONTING == "NON_FRONTING"]))
comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2025 & data_polis.3$RECLASS_TOB == "PARTNERSHIP"], na.rm = TRUE))


comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2026]))
comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2026 & data_polis.3$IS_FRONTING == "NON_FRONTING"]))
comma(sum(data_polis.3$HASIL_UW[data_polis.3$UW_YEAR == 2026 & data_polis.3$RECLASS_TOB == "PARTNERSHIP"], na.rm = TRUE))

###########################################################################

file_xls <- paste0("Hasil UW 2022-2025 as at ",unique(data_polis.3$AS_OF_DATE),".xlsx")
file_rds <- paste0("Hasil UW 2022-2025 as at ",unique(data_polis.3$AS_OF_DATE),".rds")

setwd("D:/RIDWAN/2.1 ANALISA PORTFOLIO All COB 5 UW Year Terakhir/2026/202601")
write_xlsx(data_polis.3, file_xls)
saveRDS(data_polis.3, file_rds)

###########################################################################

data.2025 <- data_polis.2 %>% 
  filter(
    UW_YEAR == 2025
  )

data.2025.summary <- data.2025 %>% 
  group_by(COB) %>% 
  summarise(
    GWP        = sum(PREMIUM, na.rm = T) - sum(DISKON, na.rm = T),
    RI_PREMIUM = - sum(RI_PREMIUM, na.rm = T),
    RI_COMM    = sum(RI_COMM, na.rm = T),
    KOMISI     = -sum(KOMISI, na.rm = T),
    OVR_IDR    = -sum(OVR_IDR, na.rm = T),
    UPR        = -sum(UPR, na.rm = T),
    GROSS_CLAIM = -sum(GROSS_CLAIM, na.rm = T),
    NET_CLAIM   = -sum(NET_CLAIM, na.rm = T),
    OPEX_IDR    = -sum(OPEX_IDR, na.rm = T),
    .groups = "drop"
  ) %>% 
  mutate(
    `PREMI NETO DICATAT`    = GWP + RI_PREMIUM,
    `PENDAPATAN PREMI NETO` = `PREMI NETO DICATAT` + UPR,
    `KOMISI NETO`           = KOMISI + OVR_IDR + RI_COMM,
    `HASIL UW`              = `PENDAPATAN PREMI NETO` + `KOMISI NETO` + NET_CLAIM + OPEX_IDR
  ) %>% 
  select(
    COB,
    GWP,
    RI_PREMIUM,
    `PREMI NETO DICATAT`,
    UPR,
    `PENDAPATAN PREMI NETO`,
    KOMISI,
    OVR_IDR,
    RI_COMM,
    `KOMISI NETO`,
    GROSS_CLAIM,
    NET_CLAIM,
    OPEX_IDR,
    `HASIL UW`
  )


data.2025.transpose <- data.2025.summary %>%
  pivot_longer(
    cols = -COB,
    names_to = "Metric",
    values_to = "Value"
  ) %>%
  pivot_wider(
    names_from = COB,
    values_from = Value
  ) %>% 
  select(
    Metric,
    FR,
    MV,
    MC,
    AV,
    EA,
    LB,
    PA,
    MS,
  )

policy_uw_2025 <- paste0("POLIS UW 2025 as at ",unique(data_polis.3$AS_OF_DATE),".xlsx")

write_xlsx(
  list(
    "DATA POLIS 2025" = data.2025,
    "Summary Hasil UW 2025" = data.2025.transpose),
  policy_uw_2025
)

View(data.2025.summary)
sum(data.2025.summary$`HASIL UW`)

###########################################################################

data.2024 <- data_polis.2 %>% 
  filter(
    UW_YEAR == 2024
  )

data.2024.summary <- data.2024 %>% 
  group_by(COB) %>% 
  summarise(
    GWP        = sum(PREMIUM, na.rm = T) - sum(DISKON, na.rm = T),
    RI_PREMIUM = - sum(RI_PREMIUM, na.rm = T),
    RI_COMM    = sum(RI_COMM, na.rm = T),
    KOMISI     = -sum(KOMISI, na.rm = T),
    OVR_IDR    = -sum(OVR_IDR, na.rm = T),
    UPR        = -sum(UPR, na.rm = T),
    GROSS_CLAIM = -sum(GROSS_CLAIM, na.rm = T),
    NET_CLAIM   = -sum(NET_CLAIM, na.rm = T),
    OPEX_IDR    = -sum(OPEX_IDR, na.rm = T),
    .groups = "drop"
  ) %>% 
  mutate(
    `PREMI NETO DICATAT`    = GWP + RI_PREMIUM,
    `PENDAPATAN PREMI NETO` = `PREMI NETO DICATAT` + UPR,
    `KOMISI NETO`           = KOMISI + OVR_IDR + RI_COMM,
    `HASIL UW`              = `PENDAPATAN PREMI NETO` + `KOMISI NETO` + NET_CLAIM + OPEX_IDR
  ) %>% 
  select(
    COB,
    GWP,
    RI_PREMIUM,
    `PREMI NETO DICATAT`,
    UPR,
    `PENDAPATAN PREMI NETO`,
    KOMISI,
    OVR_IDR,
    RI_COMM,
    `KOMISI NETO`,
    GROSS_CLAIM,
    NET_CLAIM,
    OPEX_IDR,
    `HASIL UW`
  )


data.2024.transpose <- data.2024.summary %>%
  pivot_longer(
    cols = -COB,
    names_to = "Metric",
    values_to = "Value"
  ) %>%
  pivot_wider(
    names_from = COB,
    values_from = Value
  ) %>% 
  select(
    Metric,
    FR,
    MV,
    MC,
    AV,
    EA,
    LB,
    PA,
    MS,
    everything()
  )

policy_uw_2024 <- paste0("POLIS UW 2024 as at ",unique(data_polis.2$AS_OF_DATE),".xlsx")

write_xlsx(
  list(
    "DATA POLIS 2024" = data.2024,
    "Summary Hasil UW 2024" = data.2024.transpose),
  policy_uw_2024
)

View(data.2024.summary)
comma(sum(data.2024.summary$`HASIL UW`))

###########################################################################

data.2023 <- data_polis.2 %>% 
  filter(
    UW_YEAR == 2023
  )

data.2023.summary <- data.2023 %>% 
  group_by(COB) %>% 
  summarise(
    GWP        = sum(PREMIUM, na.rm = T) - sum(DISKON, na.rm = T),
    RI_PREMIUM = - sum(RI_PREMIUM, na.rm = T),
    RI_COMM    = sum(RI_COMM, na.rm = T),
    KOMISI     = -sum(KOMISI, na.rm = T),
    OVR_IDR    = -sum(OVR_IDR, na.rm = T),
    UPR        = -sum(UPR, na.rm = T),
    GROSS_CLAIM = -sum(GROSS_CLAIM, na.rm = T),
    NET_CLAIM   = -sum(NET_CLAIM, na.rm = T),
    OPEX_IDR    = -sum(OPEX_IDR, na.rm = T),
    .groups = "drop"
  ) %>% 
  mutate(
    `PREMI NETO DICATAT`    = GWP + RI_PREMIUM,
    `PENDAPATAN PREMI NETO` = `PREMI NETO DICATAT` + UPR,
    `KOMISI NETO`           = KOMISI + OVR_IDR + RI_COMM,
    `HASIL UW`              = `PENDAPATAN PREMI NETO` + `KOMISI NETO` + NET_CLAIM + OPEX_IDR
  ) %>% 
  select(
    COB,
    GWP,
    RI_PREMIUM,
    `PREMI NETO DICATAT`,
    UPR,
    `PENDAPATAN PREMI NETO`,
    KOMISI,
    OVR_IDR,
    RI_COMM,
    `KOMISI NETO`,
    GROSS_CLAIM,
    NET_CLAIM,
    OPEX_IDR,
    `HASIL UW`
  )


data.2023.transpose <- data.2023.summary %>%
  pivot_longer(
    cols = -COB,
    names_to = "Metric",
    values_to = "Value"
  ) %>%
  pivot_wider(
    names_from = COB,
    values_from = Value
  ) %>% 
  select(
    Metric,
    FR,
    MV,
    MC,
    AV,
    EA,
    LB,
    PA,
    MS,
    everything()
  )

policy_uw_2023 <- paste0("POLIS UW 2023 as at ",unique(data_polis.2$AS_OF_DATE),".xlsx")

write_xlsx(
  list(
    "DATA POLIS 2023" = data.2023,
    "Summary Hasil UW 2023" = data.2023.transpose),
  policy_uw_2023
)

View(data.2023.summary)
comma(sum(data.2023.summary$`HASIL UW`))

###########################################################################

data.2022 <- data_polis.2 %>% 
  filter(
    UW_YEAR == 2022
  )

data.2022.summary <- data.2022 %>% 
  group_by(COB) %>% 
  summarise(
    GWP        = sum(PREMIUM, na.rm = T) - sum(DISKON, na.rm = T),
    RI_PREMIUM = - sum(RI_PREMIUM, na.rm = T),
    RI_COMM    = sum(RI_COMM, na.rm = T),
    KOMISI     = -sum(KOMISI, na.rm = T),
    OVR_IDR    = -sum(OVR_IDR, na.rm = T),
    UPR        = -sum(UPR, na.rm = T),
    GROSS_CLAIM = -sum(GROSS_CLAIM, na.rm = T),
    NET_CLAIM   = -sum(NET_CLAIM, na.rm = T),
    OPEX_IDR    = -sum(OPEX_IDR, na.rm = T),
    .groups = "drop"
  ) %>% 
  mutate(
    `PREMI NETO DICATAT`    = GWP + RI_PREMIUM,
    `PENDAPATAN PREMI NETO` = `PREMI NETO DICATAT` + UPR,
    `KOMISI NETO`           = KOMISI + OVR_IDR + RI_COMM,
    `HASIL UW`              = `PENDAPATAN PREMI NETO` + `KOMISI NETO` + NET_CLAIM + OPEX_IDR
  ) %>% 
  select(
    COB,
    GWP,
    RI_PREMIUM,
    `PREMI NETO DICATAT`,
    UPR,
    `PENDAPATAN PREMI NETO`,
    KOMISI,
    OVR_IDR,
    RI_COMM,
    `KOMISI NETO`,
    GROSS_CLAIM,
    NET_CLAIM,
    OPEX_IDR,
    `HASIL UW`
  )


data.2022.transpose <- data.2022.summary %>%
  pivot_longer(
    cols = -COB,
    names_to = "Metric",
    values_to = "Value"
  ) %>%
  pivot_wider(
    names_from = COB,
    values_from = Value
  ) %>% 
  select(
    Metric,
    FR,
    MV,
    MC,
    AV,
    EA,
    LB,
    PA,
    MS,
    everything()
  )

policy_uw_2022 <- paste0("POLIS UW 2022 as at ",unique(data_polis.2$AS_OF_DATE),".xlsx")

write_xlsx(
  list(
    "DATA POLIS 2022" = data.2022,
    "Summary Hasil UW 2022" = data.2022.transpose),
  policy_uw_2022
)

View(data.2022.summary)
comma(sum(data.2022.summary$`HASIL UW`))

###########################################################################

data.agent <- data_polis.2 %>% 
  filter(
    RECLASS_TOB == "AGENT",
    UW_YEAR == 2025
  ) %>% 
  mutate(
    GWP = PREMIUM - DISKON,
    HASIL_UW = coalesce(NWP - UPR - OPEX_IDR - NET_CLAIM,0)
  ) %>% 
  relocate(
    GWP, .after = "DISKON"
  ) %>% 
  select(
    -DAYS_2022,
    -DAYS_2023,
    -DAYS_2024,
    -DAYS_2025_1,
    -DAYS_2025_2,
    -DAYS_2026,
    -DAYS_2027,
    -DAYS_2028,
    -DAYS_2029,
    -DAYS_2030
  )
####################################
summary.cob <- data.agent %>% 
  group_by(
    COB
  ) %>% 
  summarise(
    GWP = sum(GWP)
  ) %>% 
  arrange(desc(GWP))
####################################
summary.branch <- data.agent %>% 
  group_by(
    BRANCH,
    COB
  ) %>% 
  summarise(
    GWP = sum(GWP),
    .groups = "drop"
  ) %>% 
  arrange(desc(GWP))

summary.branch.total <- summary.branch %>% 
  group_by(
    BRANCH,
  ) %>% 
  summarise(
    GWP = sum(GWP),
    .groups = "drop"
  ) %>% 
  arrange(desc(GWP))

summary.branch.1 <- summary.branch %>% 
  pivot_wider(
    names_from  = COB,
    values_from = GWP,
    values_fill = 0
  ) %>% 
  left_join(summary.branch.total, by = "BRANCH") %>% 
  relocate(
    GWP, .after = "BRANCH"
  ) %>% 
  arrange(desc(GWP))


####################################

summary.agent <- data.agent %>% 
  group_by(
    ACCOUNT_NAME_PROXY,
    COB
  ) %>% 
  summarise(
    GWP = sum(GWP)
  ) %>% 
  arrange(desc(GWP))

summary.agent.total <- summary.agent %>% 
  group_by(
    ACCOUNT_NAME_PROXY,
  ) %>% 
  summarise(
    GWP = sum(GWP)
  ) %>% 
  arrange(desc(GWP))

summary.agent.1 <- summary.agent %>% 
  pivot_wider(
    names_from  = COB,
    values_from = GWP,
    values_fill = 0
  ) %>% 
  left_join(summary.agent.total, by = "ACCOUNT_NAME_PROXY") %>% 
  relocate(
    GWP, .after = "ACCOUNT_NAME_PROXY"
  ) %>% 
  arrange(desc(GWP))

####################################
summary.agent.huw <- data.agent %>% 
  group_by(
    ACCOUNT_NAME_PROXY,
    COB
  ) %>% 
  summarise(
    HASIL_UW = sum(HASIL_UW),
    .groups = "drop"
  ) %>% 
  arrange(desc(HASIL_UW))

summary.agent.huw.total <- summary.agent.huw %>% 
  group_by(
    ACCOUNT_NAME_PROXY,
  ) %>% 
  summarise(
    HASIL_UW = sum(HASIL_UW)
  ) %>% 
  arrange(desc(HASIL_UW))

summary.agent.huw.1 <- summary.agent.huw %>% 
  pivot_wider(
    names_from  = COB,
    values_from = HASIL_UW,
    values_fill = 0
  ) %>% 
  left_join(summary.agent.huw.total, by = "ACCOUNT_NAME_PROXY") %>% 
  relocate(
    HASIL_UW, .after = "ACCOUNT_NAME_PROXY"
  ) %>% 
  arrange(desc(HASIL_UW))
####################################

data.his <- data_polis.2 %>% 
  filter(
    RECLASS_TOB == "AGENT",
    UW_YEAR >= 2023
  ) %>% 
  mutate(
    GWP = PREMIUM - DISKON,
    HASIL_UW = coalesce(NWP - UPR - OPEX_IDR - NET_CLAIM,0)
  ) %>% 
  relocate(
    GWP, .after = "DISKON"
  ) %>% 
  select(
    -DAYS_2022,
    -DAYS_2023,
    -DAYS_2024,
    -DAYS_2025_1,
    -DAYS_2025_2,
    -DAYS_2026,
    -DAYS_2027,
    -DAYS_2028,
    -DAYS_2029,
    -DAYS_2030
  )
####################################
data.his.acc <- data.his %>% 
  group_by(
    ACCOUNT_NAME_PROXY,
    UW_YEAR
  ) %>% 
  summarise(
    GWP = sum(GWP),
    GROSS_CLAIM = sum(GROSS_CLAIM),
    .groups = "drop"
  )

data.his.acc.wide <- data.his.acc %>%
  pivot_wider(
    names_from  = UW_YEAR,
    values_from = c(GWP, GROSS_CLAIM),
    values_fill = 0
  ) %>% 
  arrange(desc(GROSS_CLAIM_2025))
####################################

data.his.brnch <- data.his %>% 
  group_by(
    BRANCH,
    UW_YEAR
  ) %>% 
  summarise(
    GWP = sum(GWP),
    GROSS_CLAIM = sum(GROSS_CLAIM),
    .groups = "drop"
  )

data.his.brnch.wide <- data.his.brnch %>%
  pivot_wider(
    names_from  = UW_YEAR,
    values_from = c(GWP, GROSS_CLAIM),
    values_fill = 0
  ) %>% 
  arrange(desc(GROSS_CLAIM_2025))
####################################
####################################

write_xlsx(
  list(
    "Data"           = data.agent,
    "by COB"         = summary.cob,
    "by Branch"      = summary.branch.1,
    "by Production"  = summary.agent.1,
    "by Hasil UW"                = summary.agent.huw.1,
    "claim trend - intermediary" = data.his.acc.wide,
    "claim trend - branch"       = data.his.brnch.wide
  ),
  path = "Hasil Underwriting - Agent 2025.xlsx"
)
