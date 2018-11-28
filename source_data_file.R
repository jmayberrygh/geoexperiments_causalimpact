suppressMessages(library(data.table))
suppressMessages(library(RANN))
#' @title Source Data for  Market Analysis 
#'
#' @details  
#' This analysis was performed by analyzing GH New Diners, DAGs, and New Visitor Session Information 
#' 
#' - For test market in the input dataset, find the best control markets using time series matching. THIS IS NOT CURRENTLY REFLECTED IN THIS DOC BUT SHOULD BE IN THE NEXT VERSION 
#' 
#' - Given a test market and a matching control marketS (from above) and test market specific data, analyze the causal impact of an intervention
#' 
#' The package utilizes the dtw package in CRAN to do the time series matching, and the CausalImpact package to do the inference. 
#' (Created by Kay Brodersen at Google). For more information about the CausualImpact package, see the following reference:  
#' 
#' CausalImpact version 1.0.3, Brodersen et al., Annals of Applied Statistics (2015). http://google.github.io/CausalImpact/
#' 
#' The Zoo Creation source file has two separate functions to perform the tasks described above:
#' 
#' - zoo_creation.nds_dags(): Creates zoo DFs for NDs or DAGS or both. Typeical usage is calling zoo_creation.nds_dags(c('dags','nds'), '<modified cbsa name of test city>')
#' 
#' - zoo_creation.session():  Creates zoo DFs for session_count, session_orders,searched_and_ordered_sessions, conversion_rate. Typical call is zoo_creation.session('all','Washington-Arlington-Alexandria DC-VA-MD-WV' ), but can also specificy specific metric desired. 
#'        -This data is built off  https://stash.grubhub.com/projects/DATA/repos/etl_tasks/browse/tasks_etl2/finance_reporting/new_diner_exit_rate.sql
#'         so limited to data from past 240 Days.
#'          
#' For more details, check out the vignette: browseVignettes("MarketMatching")
#' @author Jackson Mayberry (jmayerry at grubhub.com)
#' @keywords time warping, time series matching, causal impact, zoo
#' @docType package
#' @description 
#' For a given test market find the best matching control markets using time series matching and analyze the impact of an intervention.
#' The intervention could be be a marketing event or some other local business tactic that is being tested. 
#' The package utilizes dynamic time warping to do the matching and the CausalImpact package to analyze the causal impact. 
#' 
#' 
#' 
#' 
#'
#'
#' @name MarketMatching
#' 

#################### Library Loads #################### 
library("Matching")
library("pacman")
library("kableExtra")
library("knitr")
library("MatchIt")
library("tidyr")
require("RPostgreSQL")
library("DBI")
library("dplyr")
library("ggplot2")
library("CausalImpact")
library("grid")
library(zoo)
library("bsts")
library("tidyverse")
library("reshape")
library("gtrendsR")
library(devtools)
library(MarketMatching)


#################### Redshift Connection Logic#################### 
# loads the PostgreSQL driver
drv = dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
your_user<- "jmayberry"
supersecret <- "Hello1234"
con <- dbConnect(drv, dbname = "grubhub",
                 host = "grubhub.cxossxssj4ln.us-east-1.redshift.amazonaws.com", port = "5439",
                 user = your_user , password = supersecret)
#################### New Visitior Session Traffic   ##################################################################
## Not this query was sourced from Vinanya 11/01
##### 1.0: control markets 
####QUERY TO GET ALL CONTROL MARKET DATA
session_level_data_raw_all<- dbGetQuery(con,"
                                        with dates  as 
                                        (select distinct date from list_of_all_dates_ever where date  > '2017-01-01' )
                                        
                                        select 
                                        d.date as date 
                                        ,(zs.modified_cbsa_name)
                                        ,sum(de.session_count) as session_count
                                        ,sum(de.session_orders)  as  session_orders
                                        ,sum(de.searched_and_ordered_sessions) as searched_and_ordered_sessions
                                        ,sum(de.session_orders)::float/sum(de.session_count)::float*100 as conversion_rate
                                        
                                        from
                                        dates as d left join 
                                        finance_reporting.new_diner_exit_rate de on d.date = de.currdate
                                        left join public.zipcode_summary zs
                                        on de.zip = zs.zipcode
                                        left join marketing_growth_reporting.strategy_definitions sd
                                        on zs.cbsa_name=sd.modified_cbsa_name
                                        where currdate > '2017-01-01' and  zs.modified_cbsa_name in 
                                        ('Philadelphia-Camden-Wilmington PA-NJ-DE-MD',
                                        'Boston-Cambridge-Newton MA-NH',
                                        'Denver-Aurora-Lakewood CO',
                                        'Baltimore-Columbia-Towson MD',
                                        'Atlanta-Sandy Springs-Roswell GA',
                                        'San Francisco-Oakland-Hayward CA',
                                        'Chicago-Naperville-Elgin IL-IN-WI',
                                        'Miami-Fort Lauderdale-West Palm Beach FL',
                                        'Phoenix-Mesa-Scottsdale AZ',
                                        'Los Angeles-Long Beach-Anaheim CA',
                                        'Dallas-Fort Worth-Arlington TX',
                                        'San Diego-Carlsbad CA',
                                        'Detroit-Warren-Dearborn MI',
                                        'Pittsburgh PA',
                                        'Portland-Vancouver-Hillsboro OR-WA',
                                        'Raleigh NC',
                                        'Washington-Arlington-Alexandria DC-VA-MD-WV') 
                                        group by 1,2")

#################### NEW DINERS  ##################################################################
##### 1.0: Find control markets 
####QUERY TO GET ALL CONTROL MARKET DATA


nds.all_to_find_matches<- dbGetQuery(con," with nd_count as (
                     select distinct   first_order_datetime::date as first_order_time
                                     ,dfs.first_cbsaname as modified_cbsa_name
                                     ,count( distinct email) diner_count
                                     from diner_brand_first_order_summary as dfs
                                     inner join marketing_growth_reporting.strategy_definitions on dfs.first_cbsaname = strategy_definitions.modified_cbsa_name
                                     where  order_brand = 'grubhub' and strategy_definitions.strategy in ('Maintain', 'Recapture', 'Priority Establish')
                                     group by 1,2)
                                     select cy.first_order_time as date, 
                                     cy.modified_cbsa_name,
                                     cy.diner_count
                                     from nd_count as cy
                                     where cy.first_order_time < current_date -2  and cy.first_order_time > '2018-02-18' -400
                                     group by 1,2,3")

analysis.start_date <-min(session_level_data_raw_all$date)
mm.nds  <- best_matches(data=nds.all_to_find_matches, id="modified_cbsa_name",
                       date_variable="date",
                       matching_variable="diner_count",
                       parallel=TRUE,
                       warping_limit=1, # warping limit=1
                       #dtw emphasis says how much of the rank you want to give to DTW and how much you want to give to correlation
                       dtw_emphasis=.75, # rely only on dtw for pre-screening
                       matches=20 , # request 5 matches
                       start_match_period=analysis.start_date,
                       end_match_period=test.start_date)

best_matches.nds<-data.frame(mm.nds$BestMatches)
df.matches.test_cbsa.nds<-subset(best_matches.nds, modified_cbsa_name == test_cbsa)
vec.matches.test_cbsa.nds<-df.matches.test_cbsa.nds[,2]

nds.raw<- dbGetQuery(con,paste("
                     with nd_count as (
                     select distinct   first_order_datetime::date as first_order_time
                     ,dfs.first_cbsaname as modified_cbsa_name
                     ,count( distinct email) diner_count
                     from diner_brand_first_order_summary as dfs                              
                     where (first_cbsaname IN  ('",paste(vec.matches.test_cbsa.nds,collapse = "','"),"') or modified_cbsa_name = '", test_cbsa,"')
                     and order_brand = 'grubhub'
                     group by 1,2)
                     
                     
                     
                     select cy.first_order_time as date, 
                     cy.modified_cbsa_name,
                     cy.diner_count
                     from nd_count as cy
                     
                     where cy.first_order_time < current_date -2  and cy.first_order_time > '2018-02-18' -400
                     group by 1,2,3", sep = ""))

#################### DATA PULLS USED BY ALL FILES ##########################
##########      RESTAURANT COUNTS
###QUERY TO GET TEST MARKET RESTO COUNTS BY DAY

test.market.resto_count<- dbGetQuery(con,paste("with resto_data as 
                                               (select 
                                               snapshot_date as date
                                               ,coalesce(count(distinct uld_id),0)  as total_resto_count
                                               ,coalesce(sum(is_live_grubhub),0) as gh_restos
                                               ,coalesce(sum(is_live_grubhub),0)::float/(coalesce(count(distinct uld_id),0)+1)::float as gh_percent_coverage
                                               ,coalesce(sum(is_live_uber_eats),0) as uber_eat_resto_count
                                               ,coalesce(sum(is_live_uber_eats),0)::float/(coalesce(count(distinct uld_id),0)+1)::float as ue_percent_coverage
                                               from 
                                               public.restaurant_summary_history as rs                                     
                                               inner join zipcode_summary as zs 
                                               on rs.zip = zs.zipcode
                                               where snapshot_date between '2015-01-01' and current_date - 2
                                               and 
                                               zs.modified_cbsa_name = '", test_cbsa,"'
                                               group by 1)
                                               
                                               select 
                                               dates.date as date
                                               ,rd.total_resto_count
                                               ,rd.gh_percent_coverage
                                               ,rd.uber_eat_resto_count
                                               ,rd.ue_percent_coverage
                                               from 
                                               list_of_all_dates_ever as dates
                                               left join 
                                               resto_data as rd
                                               on dates.date = rd.date 
                                               where dates.date between '2015-01-01' and current_date - 2                                    
                                               group by 1,2,3,4,5   ", sep = ""))


test_market.weather_data<- dbGetQuery(con,paste(" with   
                                                nd_concessions as (

                                                select first_order_datetime::date as concession_date,
                                               sum(first_promotion_concession_amount) as first_promo_amt
                                                 from diner_brand_first_order_summary as dfs
                                                 where  order_brand = 'grubhub' /*and first_cbsaname = '", test_cbsa,"'*/
                                                  group by 1),

                                                min_fees as (
                                                select 
                                                date_time_local::date as min_fee_date
                                                ,coalesce(avg(min_order),0) as avg_min_order
                                                ,coalesce(avg(delivery_searchpage_fee),0) as search_page_fee
                                                ,coalesce(avg((eta_max)::float),0) as avg_eta_max
                                                ,coalesce(avg(eta_min::float),0) as avg_eta_min
                                                ,coalesce(avg(eta_max::float -eta_min::float),0) as avg_eta_diff 
                                                from marketing_reporting.competitor_search_metrics_detail
                                                where trim(market)  = '", test_cbsa,"'
                                                and trim(website) = 'GrubHub'
                                                and position < 11
                                                group by 1),
                                                weather as (select wd.cbsa_name,  wd.date::date as weather_date ,wd.average_temperature
                                                ,wd.average_humidity
                                                ,wd.is_holiday
                                                ,wd.devia_from_past3day_daytime_avg_temperature 
                                                ,wd.devia_from_past3day_avg_inchesofrain
                                                ,case 
                                                when 
                                                wd.midday_heatindex >= 80 and wd.daytime_temp >= 80
                                                then (wd.midday_heatindex - wd.average_temperature)^3
                                                else 0 
                                                end
                                                as stickiness,
                                                case
                                                when (wd.rain_during_lunch) = 'light' then 1
                                                when (wd.rain_during_lunch) = 'heavy' then 2
                                                when (wd.rain_during_lunch) = 'none' then 0
                                                else 0 
                                                end
                                                as rain_during_lunch,
                                                case
                                                when (wd.rain_during_dinner) = 'light' then 1
                                                when (wd.rain_during_dinner) = 'heavy' then 2
                                                when (wd.rain_during_dinner) = 'none' then 0
                                                else 0 
                                                end
                                                as rain_during_dinner,
                                                case
                                                when (wd.snow_during_lunch) = 'light' then 1
                                                when (wd.snow_during_lunch) = 'heavy' then 1
                                                when (wd.snow_during_lunch) = 'none' then 0
                                                else 0 
                                                end
                                                as snow_during_lunch,
                                                case
                                                when (wd.snow_during_dinner) = 'light' then 1
                                                when (wd.snow_during_dinner) = 'heavy' then 1
                                                when (wd.snow_during_dinner) = 'none' then 0
                                                else 0 
                                                end
                                                as snow_during_dinner
                                                ,wd.inches_of_rain  
                                                ,wd.inches_of_snow
                                                ,wd.hours_of_rain
                                                ,wd.hours_of_snow
                                                ,wd.avg_wind_speed_mph
                                                ,wd.avg_precip_in
                                                ,wd.midday_heatindex
                                                ,wd.lunch_time_avg_temp
                                                ,wd.evening_commute_avg_temp
                                                ,wd.past3day_avg_temperature
                                                ,wd.devia_from_past3day_avg_inchesofsnow
                                                ,wd.devia_from_past3day_avg_temperature
                                                from   model_intermediate.order_daily as wd
                                                
                                                where cbsa_name  = '", test_cbsa,"'                                                 
                                                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24),
                                                conversion  as (
                                                select sum(de.session_orders)::float/sum(de.session_count)::float as conversion_rate,
                                                de.currdate as date 
                                                
                                                from
                                                finance_reporting.new_diner_exit_rate de
                                                left join public.zipcode_summary zs
                                                on de.zip = zs.zipcode
                                                left join marketing_growth_reporting.strategy_definitions sd
                                                on zs.cbsa_name=sd.modified_cbsa_name
                                                where currdate > '2017-01-01' and  zs.modified_cbsa_name =  '", test_cbsa,"'
                                                group by 2)                                                  
                                                
                                                select       min_fees.*, weather.*, promo.first_promo_amt
                                                from  weather                                   
                                                left join min_fees on min_fees.min_fee_DATE = weather.weather_date
                                                left join nd_concessions as promo on weather.weather_date = promo.concession_date
                                                where weather_date > current_date - 650", sep = ""))

#################### DAGS ##################################################################

dags.all_to_find_matches<- dbGetQuery(con,paste("select
                      date(order_time) as date,
                                cbsaname as modified_cbsa_name,
                                count(distinct order_id) dags
                                from
                                order_summary inner join  marketing_growth_reporting.strategy_definitions as sd on
                                order_summary.cbsaname = sd.modified_cbsa_name
                                WHERE
                                raw_order_brand = 'grubhub' and sd.strategy in ('Maintain', 'Recapture', 'Priority Establish')
                                and order_time > '", analysis.start_date,"'
                                
                                group by 1,2", sep = ""))



mm.dags  <- best_matches(data=dags.all_to_find_matches, id="modified_cbsa_name",
                        date_variable="date",
                        matching_variable="dags",
                        parallel=TRUE,
                        warping_limit=1, # warping limit=1
                        #dtw emphasis says how much of the rank you want to give to DTW and how much you want to give to correlation
                        dtw_emphasis=.75, # rely only on dtw for pre-screening
                        matches=20 , # request 5 matches
                        start_match_period=analysis.start_date,
                        end_match_period=test.start_date)

best_matches.dags<-data.frame(mm.dags$BestMatches)
df.matches.test_cbsa.dags<-subset(best_matches.dags, modified_cbsa_name == test_cbsa)
vec.matches.test_cbsa.dags<-df.matches.test_cbsa.dags[,2]
##### 1.0: control markets
####QUERY TO GET ALL CONTROL MARKET DATA
dags.raw<- dbGetQuery(con,paste("select
                      date(order_time) as date,
                      cbsaname as modified_cbsa_name,
                      count(distinct order_id) dags
                      from
                      order_summary inner join  marketing_growth_reporting.strategy_definitions as sd on
                      order_summary.cbsaname = sd.modified_cbsa_name
                      WHERE
                      (cbsaname IN  ('",paste(vec.matches.test_cbsa.dags,collapse = "','"),"') or cbsaname = '", test_cbsa,"')
                      and raw_order_brand = 'grubhub'
                      and order_time > '", analysis.start_date,"'
                      
                      group by 1,2", sep = ""))
#################### New Visitior Session Traffic   ##################################################################
## Not this query was sourced from Vinanya 11/01
##### 1.0: control markets 
####QUERY TO GET ALL CONTROL MARKET DATA
session_level_data_raw_all<- dbGetQuery(con,paste("
                                        with dates  as 
                                        (select distinct date from list_of_all_dates_ever where date  > '2017-01-01' )
                                        
                                        select 
                                        d.date as date 
                                        ,(zs.modified_cbsa_name)
                                        ,sum(de.session_count) as session_count
                                        ,sum(de.session_orders)  as  session_orders
                                        ,sum(de.searched_and_ordered_sessions) as searched_and_ordered_sessions
                                        ,sum(de.session_orders)::float/sum(de.session_count)::float*100 as conversion_rate
                                        
                                        from
                                        dates as d left join 
                                        finance_reporting.new_diner_exit_rate de on d.date = de.currdate
                                        left join public.zipcode_summary zs
                                        on de.zip = zs.zipcode
                                        left join marketing_growth_reporting.strategy_definitions sd
                                        on zs.cbsa_name=sd.modified_cbsa_name
                                        where currdate > '2017-01-01' and  (zs.modified_cbsa_name IN  ('",paste(vec.matches.test_cbsa,collapse = "','"),"') or zs.modified_cbsa_name = '", test_cbsa,"')
                                        group by 1,2", sep = ""))
