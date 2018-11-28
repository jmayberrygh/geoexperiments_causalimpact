


zoo_creation.session<-function(metric_desired, test_cbsa) { 
  ##Process 1 create df with current date field test market performance metric first followed by all control market performance 
  #limit raw data frame to only refelct metric testing for 
  print(metric_desired)
  print(test_cbsa)
  all_metrics<-c("session_count","session_orders","searched_and_ordered_sessions", "conversion_rate")
 
  if (metric_desired == 'all') {
    for  ( i in 1:length(all_metrics)) {
      all_metrics<-c("session_count","session_orders","searched_and_ordered_sessions", "conversion_rate")
      metric_desired <- all_metrics[[i]]
      metrics_to_be_dropped <-all_metrics[!all_metrics %in% metric_desired]
      session_level_data_1_metric<-session_level_data_raw_all[ , !(names(session_level_data_raw_all) %in% metrics_to_be_dropped)]
      #pull out_data for test market 
      session_level_data_1_metric.test_cbsa<-subset(session_level_data_1_metric, modified_cbsa_name == test_cbsa)
      #delete cbsa field from test market df
      cbsa_drop_vector<-("modified_cbsa_name")
      session_level_data_1_metric.test_cbsa <-session_level_data_1_metric.test_cbsa[!(names(session_level_data_1_metric.test_cbsa) %in% cbsa_drop_vector)]
      #delete test market data from control market performance volume 
      session_level_data_1_metric<-subset(session_level_data_1_metric, modified_cbsa_name != test_cbsa)
      #spread control market performance 
      session_level_data_1_metric.contol_market_performance_spread<- cast(session_level_data_1_metric, date ~ modified_cbsa_name )
      #merge test market data nd control market data 
      test_cbsa_data.merged_w_control_markets<-merge(session_level_data_1_metric.test_cbsa,session_level_data_1_metric.contol_market_performance_spread,by="date")
      #return(test_cbsa_data.merged_w_control_markets)
      ##Phase 2 Merge Performance data with market information 
      #merge test market and control market data with weather, min_fee_data
      test_cbsa_data.merged_w_control_markets<-merge(test_cbsa_data.merged_w_control_markets,test_market.weather_data,by.x ="date",by.y="weather_date",  all.x= T)
      weather_data_column_drops <- c('min_fee_date','cbsa_name','weather_date')
      test_cbsa_data.merged_w_control_markets <-test_cbsa_data.merged_w_control_markets[!(names(test_cbsa_data.merged_w_control_markets) %in% weather_data_column_drops)]
      #return(head(test_cbsa_data.merged_w_control_markets))
      test_cbsa_data.merged_w_control_markets_restos<-merge(test_cbsa_data.merged_w_control_markets,test.market.resto_count,by ="date")
      test_cbsa_data.merged_w_control_markets.clean<-(test_cbsa_data.merged_w_control_markets_restos)
      #set any nas to 0 
      test_cbsa_data.merged_w_control_markets_restos[is.na(test_cbsa_data.merged_w_control_markets_restos)] <- 0
      test_cbsa_data.merged_w_control_markets.clean.zoo<-read.zoo(test_cbsa_data.merged_w_control_markets_restos,  FUN = as.Date)
      #
      assign(paste("test_cbsa_zoo", all_metrics[[i]], sep = '_'), test_cbsa_data.merged_w_control_markets.clean.zoo, envir = .GlobalEnv)
    }}  else {
      all_metrics<-c("session_count","session_orders","searched_and_ordered_sessions", "conversion_rate")
      metric_desired 
      metrics_to_be_dropped <-all_metrics[!all_metrics %in% metric_desired]
      session_level_data_1_metric<-session_level_data_raw_all[ , !(names(session_level_data_raw_all) %in% metrics_to_be_dropped)]
      #pull out_data for test market 
      session_level_data_1_metric.test_cbsa<-subset(session_level_data_1_metric, modified_cbsa_name == test_cbsa)
      #delete cbsa field from test market df
      cbsa_drop_vector<-("modified_cbsa_name")
      session_level_data_1_metric.test_cbsa <-session_level_data_1_metric.test_cbsa[!(names(session_level_data_1_metric.test_cbsa) %in% cbsa_drop_vector)]
      #delete test market data from control market performance volume 
      session_level_data_1_metric<-subset(session_level_data_1_metric, modified_cbsa_name != test_cbsa)
      #spread control market performance 
      session_level_data_1_metric.contol_market_performance_spread<- cast(session_level_data_1_metric, date ~ modified_cbsa_name )
      #merge test market data nd control market data 
      test_cbsa_data.merged_w_control_markets<-merge(session_level_data_1_metric.test_cbsa,session_level_data_1_metric.contol_market_performance_spread,by="date")
      #return(test_cbsa_data.merged_w_control_markets)
      ##Phase 2 Merge Performance data with market information 
      #merge test market and control market data with weather, min_fee_data
      test_cbsa_data.merged_w_control_markets<<-merge(test_cbsa_data.merged_w_control_markets,test_market.weather_data,by.x ="date", by.y = "weather_date")
      weather_data_column_drops <- c('min_fee_date','cbsa_name','weather_date')
      test_cbsa_data.merged_w_control_markets <-test_cbsa_data.merged_w_control_markets[!(names(test_cbsa_data.merged_w_control_markets) %in% weather_data_column_drops)]
      #return(head(test_cbsa_data.merged_w_control_markets))
      test_cbsa_data.merged_w_control_markets_restos<-merge(test_cbsa_data.merged_w_control_markets,test.market.resto_count,by ="date")
      test_cbsa_data.merged_w_control_markets.clean<-(test_cbsa_data.merged_w_control_markets_restos)
      #set any nas to 0 
      test_cbsa_data.merged_w_control_markets_restos[is.na(test_cbsa_data.merged_w_control_markets_restos)] <- 0
      test_cbsa_data.merged_w_control_markets.clean.zoo<<--read.zoo(test_cbsa_data.merged_w_control_markets_restos,  FUN = as.Date)
      #
      assign(paste("test_cbsa_zoo", metric_desired, sep = '_'), test_cbsa_data.merged_w_control_markets.clean.zoo, envir = .GlobalEnv)
    }
}
zoo_creation.nds_dags<-function(metrics_desired, test_cbsa) { 
  ##Process 1 create df with current date field test market performance metric first followed by all control market performance 
  #limit raw data frame to only refelct metric testing for 

   for  ( i in 1:length(metrics_desired)) {
  if (metrics_desired[i] == 'dags') {
    
      #pull out_data for test market 
      dags.raw.test_cbsa<-subset(dags.raw, modified_cbsa_name == test_cbsa)
      #delete cbsa field from test market df
      cbsa_drop_vector<-("modified_cbsa_name")
      dags.raw.test_cbsa <-dags.raw.test_cbsa[!(names(dags.raw.test_cbsa) %in% cbsa_drop_vector)]
      #delete test market data from control market performance volume 
      dags.raw.control<-subset(dags.raw, modified_cbsa_name != test_cbsa)
      #spread control market performance 
      dags.raw.control.spread <- cast(dags.raw.control, date ~ modified_cbsa_name )
      #merge test market data nd control market data 
      test_cbsa.merged_w_control<-merge(dags.raw.test_cbsa,dags.raw.control.spread,by="date")
      #return(test_cbsa_data.merged_w_control_markets)
      ##Phase 2 Merge Performance data with market information 
      #merge test market and control market data with weather, min_fee_data
      test_cbsa_data.merged_w_control<-merge(test_cbsa.merged_w_control,test_market.weather_data,by.x ="date",by.y="weather_date",  all.x= T)
      weather_data_column_drops <- c('min_fee_date','cbsa_name','weather_date')
      test_cbsa_data.merged_w_control <-test_cbsa_data.merged_w_control[!(names(test_cbsa_data.merged_w_control) %in% weather_data_column_drops)]
      #return(head(test_cbsa_data.merged_w_control_markets))
      test_cbsa_data.merged_w_control.restos<-merge(test_cbsa_data.merged_w_control,test.market.resto_count,by ="date")
      test_cbsa_data.merged_w_control.restos.clean<-(test_cbsa_data.merged_w_control.restos)
      #set any nas to 0 
      test_cbsa_data.merged_w_control.restos.clean[is.na(test_cbsa_data.merged_w_control.restos.clean)] <- 0
      test_cbsa_data.merged_w_control.restos.clean.zoo<-read.zoo(test_cbsa_data.merged_w_control.restos.clean,  FUN = as.Date)
      #
      assign(paste("test_cbsa_zoo", metrics_desired[i], sep = '_'), test_cbsa_data.merged_w_control.restos.clean.zoo, envir = .GlobalEnv)
    }  else if  (metrics_desired[i] == 'nds') {
      #pull out_data for test market 
      nds.raw.test_cbsa<-subset(nds.raw, modified_cbsa_name == test_cbsa)
      #delete cbsa field from test market df
      cbsa_drop_vector<-("modified_cbsa_name")
      nds.raw.test_cbsa <-nds.raw.test_cbsa[!(names(nds.raw.test_cbsa) %in% cbsa_drop_vector)]
      #delete test market data from control market performance volume 
      nds.raw.control<-subset(nds.raw, modified_cbsa_name != test_cbsa)
      #spread control market performance 
      nds.raw.control.spread <- cast(nds.raw.control, date ~ modified_cbsa_name )
      #merge test market data nd control market data 
      test_cbsa.merged_w_control<-merge(nds.raw.test_cbsa,nds.raw.control.spread,by="date")
      #return(test_cbsa_data.merged_w_control_markets)
      ##Phase 2 Merge Performance data with market information 
      #merge test market and control market data with weather, min_fee_data
      test_cbsa_data.merged_w_control<-merge(test_cbsa.merged_w_control,test_market.weather_data,by.x ="date",by.y="weather_date",  all.x= T)
      weather_data_column_drops <- c('min_fee_date','cbsa_name','weather_date')
      test_cbsa_data.merged_w_control <-test_cbsa_data.merged_w_control[!(names(test_cbsa_data.merged_w_control) %in% weather_data_column_drops)]
      #return(head(test_cbsa_data.merged_w_control_markets))
      test_cbsa_data.merged_w_control.restos<-merge(test_cbsa_data.merged_w_control,test.market.resto_count,by ="date")
      test_cbsa_data.merged_w_control.restos.clean<-(test_cbsa_data.merged_w_control.restos)
      #set any nas to 0 
      test_cbsa_data.merged_w_control.restos.clean[is.na(test_cbsa_data.merged_w_control.restos.clean)] <- 0
      test_cbsa_data.merged_w_control.restos.clean.zoo<-read.zoo(test_cbsa_data.merged_w_control.restos.clean,  FUN = as.Date)
      #
      assign(paste("test_cbsa_zoo",  metrics_desired[i], sep = '_'), test_cbsa_data.merged_w_control.restos.clean.zoo, envir = .GlobalEnv)
    }
   }   
}
#zoo_creation.nds_dags(c('dags','nds'),'Washington-Arlington-Alexandria DC-VA-MD-WV')
#zoo_creation.session('all','Washington-Arlington-Alexandria DC-VA-MD-WV')

   