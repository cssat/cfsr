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
library(magrittr)
library(dbplyr)
library(dplyr)

# establishing db connection

con <- dbConnect(odbc::odbc(), "POC")

# getting data
afcars <-
  tbl(con, in_schema("annual_report", "ca_fc_afcars_extracts")) %>%
  dplyr::select(recnumbr
                , repdat
                , rem1dt
                , dlstfcdt
                , latremdt
                , dodfcdt
                , totalrem
                , disreasn) %>%
  collect()

# dbGetQuery(con, "SELECT recnumbr
# 	                  , repdat
#                     , rem1dt
#                     , dlstfcdt
#                     , latremdt
#                     , dodfcdt
#                     , totalrem
#                     , disreasn
#                 FROM [CA_ODS].[annual_report].[ca_fc_afcars_extracts]"
#            )

date_to_integer <- function(x){as.integer(str_replace_all(as.character(x), '-', ''))}

mtr <- afcars %>%
  # get maximum number of removals for each child:
  group_by(recnumbr) %>%
  summarise(max_totalrem = max(totalrem, na.rm = TRUE)) %>%
  ungroup()

removal_episode_fact <- afcars %>%
  # rank reports by last report date for each child and removal date:
  group_by(recnumbr, latremdt) %>%
  mutate(eps_rnk = row_number(desc(repdat))) %>%
  ungroup() %>%
  # keep only last report:
  filter(eps_rnk == 1) %>% # this makes episode-level df
  arrange(recnumbr, repdat) %>%
  mutate(int_rem1dt = date_to_numeric(rem1dt) # make dates into integers
         , int_dlstfcdt = date_to_integer(dlstfcdt)
         , int_latremdt = date_to_integer(latremdt)
         , int_dodfcdt = date_to_integer(dodfcdt)
         # fill in missing discharge dates with the next episode's last discharge date:
         , int_dodfcdt = ifelse((lead(totalrem) - totalrem) == 1 & is.na(int_dodfcdt), lead(int_dlstfcdt), int_dodfcdt))  

## WILL EVENTUALLY REMOVE
## Creating flags for problematic records
## What should we do with records that have a totalrem starting at 0?
problem_records <- removal_episode_fact %>%
  group_by(recnumbr) %>%
  mutate(latremdt_flag = ifelse(lag(int_latremdt) > int_latremdt, 1, 0)
         , totalrem_flag = ifelse(totalrem - lag(totalrem) != 1 | min(totalrem) != 1, 1, 0)
  )%>%
  ungroup() %>%
  filter(latremdt_flag == 1 | totalrem_flag == 1) 
  
removal_episode_fact2 <- removal_episode_fact %>%
  ## This complete function (from tidyr) seemed like the easiest way to
  ## create the new rows, but if anyone has a better idea, let's do it!
  # expand df by adding row for each potential/unrecorded removal (0-18)  
  complete(recnumbr, totalrem = seq(1, max(totalrem))) %>%
  # join back to max_totalrem:
  left_join(mtr) %>%
  # keep only valid episodes:
  filter(totalrem <= max_totalrem) %>%
  # create unique id for each episode: 
  mutate(id_removal_epsisode_fact = row_number()) %>%
  # subset/rename data
  select(id_prsn = recnumbr
         , id_cal_dim_begin = int_latremdt
         , id_cal_dim_end = int_dodfcdt
         , id_cal_dim_date_begin = latremdt
         , id_cal_dim_date_end = dodfcdt
         , id_discharge_reason_dim = disreasn
         , repdat
         , int_dlstfcdt
         , totalrem
         , max_totalrem
  )


