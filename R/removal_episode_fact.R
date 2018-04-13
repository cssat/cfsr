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
# afcars <-
#   tbl(con, in_schema("annual_report", "ca_fc_afcars_extracts")) %>%
#   dplyr::select(recnumbr
#                 , repdat
#                 , dob
#                 , rem1dt
#                 , dlstfcdt
#                 , latremdt
#                 , dodfcdt
#                 , totalrem
#                 , disreasn) %>%
#   collect()

afcars <- dbGetQuery(con, "SELECT recnumbr
	                  , repdat
                    , dob
                    , rem1dt
                    , dlstfcdt
                    , latremdt
                    , dodfcdt
                    , totalrem
                    , disreasn
                FROM [CA_ODS].[annual_report].[ca_fc_afcars_extracts]"
           )

date_to_integer <- function(x){as.integer(str_replace_all(as.character(x), '-', ''))}

max_total_removals <- afcars %>%
  # get maximum number of removals for each child:
  group_by(recnumbr) %>%
  summarise(max_totalrem = max(totalrem, na.rm = TRUE)) %>%
  ungroup()

missing_first_removal <- afcars %>% 
  # filter where first removal is missing an episode record
  group_by(recnumbr) %>%
  filter(all(rem1dt != latremdt)) %>%
  ungroup() %>%
  # keep unique first removal dates (keep last report)
  arrange(desc(repdat)) %>%
  distinct(recnumbr, rem1dt, .keep_all = TRUE) %>%
  dplyr::select(recnumbr, repdat, dob, rem1dt) %>%
  mutate(repdat = 000000, # recode repdat
         latremdt = rem1dt) # make last removal date the first removal date


removal_episode_fact <- afcars %>%
  # add rows where first removal was missing an episode record
  bind_rows(missing_first_removal) %>%
  # rank reports by last report date for each child and removal date:
  group_by(recnumbr, latremdt) %>%
  mutate(eps_rnk = row_number(desc(repdat))) %>%
  ungroup() %>%
  # keep only last report:
  filter(eps_rnk == 1) %>% # this makes episode-level df
  group_by(recnumbr) %>%
  # count removals on episodes that exist in the table 
  # Use this moving forward? For now, keeping to totalrem
  mutate(totalrem_cnt = row_number(latremdt)) %>%
  ungroup() %>%
  arrange(recnumbr, repdat) %>%
  mutate(int_rem1dt = date_to_integer(rem1dt) # make dates into integers
         , int_dlstfcdt = date_to_integer(dlstfcdt)
         , int_latremdt = date_to_integer(latremdt)
         , int_dodfcdt = date_to_integer(dodfcdt)
         # fill in missing discharge dates with the next episode's last discharge date:
         , int_dodfcdt = ifelse((lead(totalrem) - totalrem) == 1 & is.na(int_dodfcdt), lead(int_dlstfcdt), int_dodfcdt)) %>%
## WILL EVENTUALLY REMOVE
## Creating flags for problematic records
## What should we do with records that have a totalrem starting at 0?
  group_by(recnumbr) %>%
         # Flag where previous last removal is after last removal:
  mutate(latremdt_flag = ifelse(lag(int_latremdt) > int_latremdt, 1, 0)
         # Flag where an episode is missing or where the first record of an episode is not the first removal
         , totalrem_flag = ifelse(totalrem - lag(totalrem) != 1 | min(totalrem) != 1, 1, 0)
  )%>%
  ungroup()%>%
  ## This complete function (from tidyr) seemed like the easiest way to
  ## create the new rows, but if anyone has a better idea, let's do it!
  # expand df by adding row for each potential/unrecorded removal (0-18)  
  complete(recnumbr, totalrem = seq(1, max(totalrem, na.rm = TRUE))) %>%
  # join back to max_totalrem:
  left_join(max_total_removals) %>%
  # keep only valid episodes:
  filter(totalrem <= max_totalrem) %>%
  # create unique id for each episode: 
  mutate(id_removal_epsisode_fact = row_number()) %>%
  # subset/rename data
  select(id_prsn = recnumbr
         , dob
         , id_cal_dim_begin = int_latremdt
         , id_cal_dim_end = int_dodfcdt
         , id_cal_dim_date_begin = latremdt
         , id_cal_dim_date_end = dodfcdt
         , id_discharge_reason_dim = disreasn
         , repdat
         , int_dlstfcdt
         , totalrem
         , totalrem_cnt
         , max_totalrem
  )

## still need to filter out reports outside range (2008-10-01 to 2017-09-30)


