#' @title DC Twin Market Analysis 
#'
#' @details  
#' 
#' This analysis was performed by analyzing GH New Diners, DAGs, and New Visitor Session Information 
#' 
#' - For test market in the input dataset, find the best control markets using time series matching. This is performed in the source_data_file.r script. 
#' 
#' - Given a test market and a matching control marketS (from above) and test market specific data (mostly weather), analyze the causal impact of an intervention
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
#' 
#' Analysis takes 6 steps + 1 set up step (step 0): 
#' 0: Base information. This is the only thing that a user needs to fill in to start the anlaysis. 
#' 1: Pull test information metrics and custom functions . This includes finding control markets based on DTW. 
#' 2. Create zoos for all metrics
#' 3. Eliminate any markets from control market pool based on business logic. Bleed over effects, concurrent local marketing during validation or testing periods. 
#' 4. Create time periods for analysis. This should be all progrmatic and is based off the testing dates and earliest avaliable session data. Reference link to ETL job above. Only past 240 days availabe. 
#' 5-7. Analysis for each period. Includes a validation period to ensure that we can accurately predict the metric in the 30 days leading up to the test period. The Probablity of impact shold be <.85 (our alpha value), ideally close to .5 and the confidence interval of impact contain 0. 
#' Future steps I will include an adj. R2 value and RMSE values for the pretest periods. Just need to finish writing the function. 
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

######  Step 0: Base Information ######  
list = ls()
test.start_date<-as.Date('2018-10-01')
test.end_date<-as.Date('2018-10-29')
test_cbsa<- "Washington-Arlington-Alexandria DC-VA-MD-WV"
media_spend<-99892

######  Step 1: Identify the test city and creaete source data files.###### 
#Run source code 
source("~/Documents/ANALYSIS/twin_market_production_code/source_data_file.R")
source("~/Documents/ANALYSIS/twin_market_production_code/function_to_create_zoos.R")


######  Step 2: Create Zoos for test metrics based on test city's modified cbsa name. ######
zoo_creation.nds_dags(c('dags','nds'), test_cbsa)
zoo_creation.session('all',test_cbsa )

######  Step 3: Elminate markets that were good matches but might not follow seasonal trends######
#drop_function(c('Washington-Arlington-Alexandria DC-VA-MD-WV', 'San Francisco-Oakland-Hayward CA', 'Dallas-Fort Worth-Arlington TX', 'Baltimore-Columbia-Towson MD'))

##Baltimore: Concerned about potential bleed over effects from DC to Balitmore  
##Denver: Based on feedback from Meredith/Amanda - there was a local package that kick off towwards the end of the test.
#SF has several major GHD Launch Districdts
#Seattle had fee testing during the month of September
#
View(df.matches.test_cbsa)
markets_to_be_dropped<-c('Washington-Arlington-Alexandria DC-VA-MD-WV', 'San Francisco-Oakland-Hayward CA', 'Los Angeles-Long Beach-Anaheim CA', 'Dallas-Fort Worth-Arlington TX','Seattle-Tacoma-Bellevue WA')
#markets_to_be_dropped<-c('Baltimore-Columbia-Towson MD','Denver-Aurora-Lakewood CO', 'Dallas-')
#Drop Markets and create new Zoos 
  test_cbsa_zoo_dags.dropped<-test_cbsa_zoo_dags[ , !(names(test_cbsa_zoo_dags) %in% markets_to_be_dropped)]
  test_cbsa_zoo_nds.dropped<-test_cbsa_zoo_nds[ , !(names(test_cbsa_zoo_nds) %in% markets_to_be_dropped)]
  
  

######  Step 4: Analysis Setup: Create test and validation time periods. Starting date will be basedon the earliest availabe session data ######
analysis.start_date <-min(session_level_data_raw_all$date)
analysis.end_date <-max(session_level_data_raw_all$date)
##Validation 
#Create Validation Time Periods  30 Days Leading up to Test Start Date 
pre.period.val<- as.Date(c(analysis.start_date, test.start_date-32))
post.period.val<-as.Date(c(test.start_date-31, test.start_date-1))
##Test Period: No Cool Down  
pre.period.actuals<- as.Date(c(analysis.start_date, test.start_date-1))
post.period.actuals.no_cool_down<-as.Date(c(test.start_date,test.end_date))
##Test Period: 1 week cool Down 
post.period.actuals.1_week_down<-as.Date(c(test.start_date,test.end_date+7))
##Test Period: 2 week cool Down 
post.period.actuals.2_week_down<-as.Date(c(test.start_date,test.end_date+14))
##Test Period: 4 week cool Down 
post.period.actuals.4_week_down<-as.Date(c(test.start_date,test.end_date+28))

#Decide Simualation Counts
val.sim_count<-30000
act.sim_count<-30000
val.alpha<-.15
act.alpha<-.15


######  Step 5: Run Analysis for New Diners ####### 

# Validation for 4 weeks leading up to test 
causal_impact_val_nd<-CausalImpact(window(test_cbsa_zoo_nds.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.val, post.period.val, alpha =val.alpha, model.args = list(niter = val.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
summary(causal_impact_val_nd)
plot(causal_impact_val_nd)
plot(causal_impact_val_nd$model$bsts.model, "coefficients") 
# Actuals for testing period 
causal_impact_act_nd.no_cool<-CausalImpact(window(test_cbsa_zoo_nds.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.no_cool_down, alpha =act.alpha, model.args = list(niter = act.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
summary<-summary(causal_impact_act_nd.no_cool)
plot(causal_impact_act_nd.no_cool)
plot(causal_impact_act_nd.no_cool$model$bsts.model, "coefficients") 
causal_impact_act_nd.1_week_cool<-CausalImpact(window(test_cbsa_zoo_nds.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.1_week_down, alpha =act.alpha, model.args = list(niter = act.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
causal_impact_act_nd.2_week_cool<-CausalImpact(window(test_cbsa_zoo_nds.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.2_week_down, alpha =act.alpha, model.args = list(niter = act.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
causal_impact_act_nd.4_week_cool<-CausalImpact(window(test_cbsa_zoo_nds.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.4_week_down, alpha =act.alpha, model.args = list(niter = act.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))

#Aggregate results and CPRs 


  
######  Step 6: Session Level Analysis ######  
all_metrics<-c("session_count","session_orders","searched_and_ordered_sessions", "conversion_rate")
for (i in 1:length(all_metrics)) {
  all_metrics<-c("session_count","session_orders","searched_and_ordered_sessions", "conversion_rate")
  metric_desired <- all_metrics[[i]]
  metrics_to_be_dropped <-all_metrics[!all_metrics %in% metric_desired]
  zoo_for_testing<-get(paste("test_cbsa_zoo",metric_desired,sep = '_'))
  zoo_for_testing<-zoo_for_testing[ , !(names(zoo_for_testing) %in% markets_to_be_dropped)]
  #create impact val for each session related metric 
  causal_impact_val<-CausalImpact(window(zoo_for_testing, start = analysis.start_date, end = analysis.end_date), pre.period.val, post.period.val, alpha =.15, model.args = list(niter = val.sim_count, nseasons = 7, season.duration= 1))
  assign(paste("causal_impact_val", metric_desired, sep = '_'), causal_impact_val, envir = .GlobalEnv)
  #
  causal_impact_nocooldown_act<-CausalImpact(window(zoo_for_testing, start = analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.no_cool_down, alpha =.15, model.args = list(niter = act.sim_count, nseasons = 7, season.duration= 1))
  assign(paste("causal_impact_nocooldown_act", metric_desired, sep = '_'), causal_impact_nocooldown_act, envir = .GlobalEnv)
}

## Session Count ## 
summary(causal_impact_val_session_count)
plot(causal_impact_nocooldown_act_session_count)
summary(causal_impact_nocooldown_act_session_count)

## Session_orders ## 
summary(causal_impact_val_session_orders)
summary(causal_impact_nocooldown_act_session_orders)

## Session Count ## 
summary(causal_impact_val_searched_and_ordered_sessions)
summary(causal_impact_nocooldown_act_searched_and_ordered_sessions)

## conversion_rate ## 
summary(causal_impact_val_conversion_rate)
plot(causal_impact_val_conversion_rate)
summary(causal_impact_nocooldown_act_conversion_rate)
plot(causal_impact_nocooldown_act_conversion_rate)






######  Step 7: Run Analysis for DAGS ####### 

# Validation for 4 weeks leading up to test 
causal_impact_val_dags<-CausalImpact(window(test_cbsa_zoo_dags.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.val, post.period.val, alpha =.15, model.args = list(niter = val.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
plot(causal_impact_val_dags)
plot(causal_impact_val_dags$model$bsts.model, "coefficients") 
# Actuals for testing period 
causal_impact_act_dags.no_cool<-CausalImpact(window(test_cbsa_zoo_dags.dropped, start =analysis.start_date, end = analysis.end_date), pre.period.actuals, post.period.actuals.no_cool_down, alpha =.15, model.args = list(niter = val.sim_count, nseasons = 7, season.duration= 1, dynamic.regression = FALSE))
plot(causal_impact_act_dags.no_cool)
plot(causal_impact_act_dags.no_cool$model$bsts.model, "coefficients") 




##### Scratch ##### 
######### : Accurcacy of Model 
##### S8.1 Validaion Period ### 
##set up data frame with prection and acutal values 
causal_impact_val_nd.series<-causal_impact_val_nd$series
df.causal_impact_val_nd.series <- fortify.zoo(causal_impact_val_nd.series)
View(df.causal_impact_val_nd.series)
#.80
#pre-test period
#r_squared(df.causal_impact_val_nd.series$response[df.causal_impact_val_nd.series$Index >= analysis.start_date & df.causal_impact_val_nd.series$Index <= test.start_date-30], df.causal_impact_val_nd.series$point.pred[df.causal_impact_val_nd.series$Index >= analysis.start_date & df.causal_impact_val_nd.series$Index <= test.start_date-30])
#RMSE(dags.dc.impact.prediction_df.val$response[dags.dc.impact.prediction_df.val$Index >= '2018-01-02' & dags.dc.impact.prediction_df.val$Index <= '2018-04-01'], dags.dc.impact.prediction_df.val$point.pred[dags.dc.impact.prediction_df.val$Index >= '2018-01-02' & dags.dc.impact.prediction_df.val$Index <= '2018-04-01'])


causal_impact_act_nd.series<-causal_impact_act_nd.no_cool$series
df.causal_impact_act_nd.series<- fortify.zoo(causal_impact_act_nd.series)
View(df.causal_impact_act_nd.series)
?write.csv()
#.80

