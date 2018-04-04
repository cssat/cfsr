
# COlumns needed for AFCARS EPISODE_REMOVAL_FACT
# id_removal_epsisode_fact - this is the primary key for the table
# id_prsn - recnumbr
# id_cal_dim_begin
# id_cal_dim_end
# id_cal_dim_date_begin
# id_cal_dim_date_end
# id_discharge_reason_dim

# loading required libraries

library(DBI)
library(stringr)
library(tidyr)
library(dbplyr)
library(dplyr)

# establishing db connection

con <- dbConnect(odbc::odbc(), "POC")

# getting data

## Joe suggested using tbl, I was having connection problems
## and wanted to just get the script working!
# tbl(con, "[annual_report].[ca_fc_afcars_extracts]") %>%
#   filter(recnumbr == 4021) %>% as_data_frame()

DBI::dbListTables(con)

tbl(con, in_schema("annual_report", "ca_fc_afcars_extracts"))


afcars <-
dbGetQuery(con, "SELECT recnumbr
	                  , repdat
                    , rem1dt
                    , dlstfcdt
                    , latremdt
                    , dodfcdt
                    , totalrem
                    , disreasn
                FROM [CA_ODS].[annual_report].[ca_fc_afcars_extracts]"
           )

# function!

date_to_numeric <- function(x){as.numeric(str_replace_all(as.character(x), '-', ''))}

removal_episode_fact <- 
  group_by(afcars, recnumbr, latremdt) %>%
  arrange(desc(repdat)) %>%
  mutate(eps_rnk = row_number()) %>%
  group_by(recnumbr) %>%
  mutate(max_totalrem = max(totalrem)) %>%
  ungroup() %>%
  filter(eps_rnk == 1) %>%
  # filter(recnumbr %in% c(4021, 2740417, 1418573, 1501651)) %>%
  arrange(recnumbr, repdat) %>%
  mutate(int_rem1dt = date_to_numeric(rem1dt)
         , int_dlstfcdt = date_to_numeric(dlstfcdt)
         , int_latremdt = date_to_numeric(latremdt)
         , int_dodfcdt = date_to_numeric(dodfcdt)
         , int_dodfcdt = ifelse((lead(totalrem) - totalrem) == 1 & is.na(int_dodfcdt), lead(int_dlstfcdt), int_dodfcdt)
         , id_removal_epsisode_fact = 1:n()
         ) %>%
  select(id_removal_epsisode_fact 
         , id_prsn = recnumbr
         , id_cal_dim_begin = int_latremdt
         , id_cal_dim_end = int_dodfcdt
         , id_cal_dim_date_begin = latremdt
         , id_cal_dim_date_end = dodfcdt
         , id_discharge_reason_dim = disreasn
         , repdat
         , int_dlstfcdt
         , totalrem
         , max_totalrem
  ) %>%
  ## WILL EVENTUALLY REMOVE
  ## Creating flags for problematic records
  group_by(id_prsn) %>%
  arrange(id_removal_epsisode_fact) %>%
  mutate(latremdt_flag = ifelse(lag(id_cal_dim_begin) > id_cal_dim_begin, 1, 0)
         , totalrem_flag = ifelse(totalrem - lag(totalrem) != 1, 1, 0)
         )%>%
  ungroup()

removal_episode_fact <- 
  select(removal_episode_fact, - max_totalrem) %>% 
  ## THis complete function (from tidyr) seemed like the easiest way to
  ## create the new rows, but if anyone has a better idea, let's do it!
  complete(id_prsn, totalrem = seq(min(totalrem), max(totalrem))) %>%
  left_join(select(removal_episode_fact, id_prsn, max_totalrem) %>% distinct()) %>%
  filter(totalrem <= max_totalrem)





