# Fetch Data

# Load libraries
library(dplyr)
library(tidyr)
library(lubridate)
library(RPostgreSQL)
library(ggplot2)

#----------------------- load data not in database--------------------
load("Data/sites_id.RData")

df_gauge_featureids <- NewData %>%
  dplyr::rename(featureid = FEATUREID)
rm("NewData")

# get unique featureid with obvserved trout data for db queries
feature_ids <- unique(df_gauge_featureids$featureid)

# load flow data
df_flow <- load("Data/Flows.RData")

# merge to associate featureid with flow data
df_flow <- df_flow %>%
  left_join(df_gauge_featureids) 


#------------------------Pull covariate data from database--------------

# load profile locally to play with packrat
source("~/.Rprofile")

# set connection to database
db <- src_postgres(dbname='sheds', host='felek.cns.umass.edu', port='5432', user=options('SHEDS_USERNAME'), password=options('SHEDS_PASSWORD'))

# table connection
tbl_covariates <- tbl(db, 'covariates') %>%
  dplyr::group_by(featureid) %>%
  dplyr::filter(featureid %in% feature_ids)

# collect the query and organize
df_covariates_long <- dplyr::collect(tbl_covariates) 

# upstream
df_covariates <- df_covariates_long %>%
  dplyr::filter(zone == "upstream") %>%
  tidyr::spread(variable, value)
summary(df_covariates)


#-------------------------Get HUC8 & HUC12 & add to covariate df---------------
# pass the db$con from dplyr as the connection to RPostgreSQL::dbSendQuery
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname='sheds', host='ecosheds.org', port='5432', user=options('SHEDS_USERNAME'), password=options('SHEDS_PASSWORD'))
rs <- dbSendQuery(con, "SELECT c.featureid as featureid, w.huc8 as huc8
                  FROM catchments c
                  JOIN wbdhu8 w
                  ON ST_Contains(w.geom, ST_Centroid(c.geom));")

# fetch results
featureid_huc8 <- fetch(rs, n=-1)

df_covariates <- dplyr::left_join(df_covariates, featureid_huc8, by = c("featureid"))

# HUC10
rs <- dbSendQuery(con, "SELECT c.featureid as featureid, w.huc10 as huc10
                  FROM catchments c
                  JOIN wbdhu10 w
                  ON ST_Contains(w.geom, ST_Centroid(c.geom));")

# fetch results
featureid_huc10 <- fetch(rs, n=-1)

df_covariates <- dplyr::left_join(df_covariates, featureid_huc10, by = c("featureid"))

# HUC12
rs <- dbSendQuery(con, "SELECT featureid, huc12
                  FROM catchment_huc12;")

# fetch results
featureid_huc12 <- fetch(rs, n=-1)

df_covariates <- dplyr::left_join(df_covariates, featureid_huc12, by = c("featureid"))
