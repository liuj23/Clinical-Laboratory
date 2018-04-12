library(shiny)
library(ggplot2)  
library(DT)
library(zoo)
library(forecast)
library(rsconnect)
library(date)



process_data <- function(filename){
  MyData <- read.csv(filename )
  x<-MyData[,1]
  MyDate<-MyData[,2]
  y<<-MyData[,3]
  EstimatedPoints = 75
  Points_In_Pattern = 12
  dataFreq = "Month of the year"
  StartDate = MyDate[1]
  
  Number_PointsToEstimate <<- EstimatedPoints
  patternFreq<<- Points_In_Pattern
  #######################################################
  RecordedTravelTime<-y# scan("C:/Users/Emad/Dropbox/CLL_BloodImages/TravelTimeEstimation/R Code/TraveltimeData/700.dat")
  # Dickey-Fuller test for variable  y_{t}=(\rho-1)y_{t-1}+u_{t}=\delta y_{t-1}+u_{t}\, 
  #adf.test(RecordedTravelTime, alternative="stationary", k=0)
  Number_PointsToEstimate <<- EstimatedPoints
  patternFreq<<- Points_In_Pattern
  recordsDate<<- as.character(StartDate)
  recordsDate<<-as.POSIXlt(recordsDate)
  recordsDate<<-unclass (recordsDate)
  paste("date is", recordsDate$year)
  year<<-recordsDate$year+1900
  mon<<-recordsDate$mon
  if (dataFreq=="Minute")   {SeriesFreq<-60}
  if (dataFreq=="Hour")     {SeriesFreq<-24}
  if (dataFreq=="Day of the week")      {SeriesFreq<-7}
  if (dataFreq=="Week")     {SeriesFreq<-52}
  if (dataFreq=="Two-Weeks"){SeriesFreq<-26}
  if (dataFreq=="Month of the year")    {SeriesFreq<-12}
  if (dataFreq=="Year")     {SeriesFreq<-1}
  


  TSeries<<-ts(RecordedTravelTime,  frequency=SeriesFreq,start=c(year,mon+1))#start=c(2011,4), delta=1
  #                                l    = mean(RecordedTravelTime[1:patternFreq])
  #                                b    = (RecordedTravelTime[patternFreq]-RecordedTravelTime[1])/(patternFreq-1)
  #                                sMul = RecordedTravelTime[1:patternFreq]/mean(RecordedTravelTime[1:patternFreq]) 
  #                  sAdd = RecordedTravelTime[1:patternFreq]-mean(RecordedTravelTime[1:patternFreq]) 
  #########################HWMultModel#################################################
  TforecastsHWAdd <<- HoltWinters(TSeries, seasonal="mult",start.periods = patternFreq,
                                  optim.start = c(alpha = 0.3, beta = 0.1,gamma = 0.1)) # l.start=l,b.start=b,s.start=sMul,
  #####################################################################################
  xhatHWAdd<-TforecastsHWAdd$fitted[,1]
  ResidualErrorsHWAdd<-residuals(TforecastsHWAdd)
  Tmean<-mean(RecordedTravelTime)
  
  ResidualErrorsHWAdd<-residuals(TforecastsHWAdd)
  SumSEHWAdd<<-sum((xhatHWAdd-Tmean)^2)
  SSRHWAdd<-sum ((RecordedTravelTime[(patternFreq+1):length(RecordedTravelTime)]-Tmean)^2) 
  RsequaredHWAdd<<-SumSEHWAdd/SSRHWAdd
  
  dummy <- vector(mode="numeric", length=patternFreq)
  nxhatHWAdd<- c(dummy,xhatHWAdd)
  nresidulasHWAdd<-c(dummy,ResidualErrorsHWAdd)
  pAdd <<- predict(TforecastsHWAdd, Number_PointsToEstimate, prediction.interval = TRUE, level = 0.95)
  Estimated_ValueHWAdd <<- pAdd[,1]
  UpperLimitHWAdd      <<- pAdd[,2]
  LowerLimitHWAdd      <<- pAdd[,3]
  PrecisionHWAdd       <<- (UpperLimitHWAdd-LowerLimitHWAdd)/2
  
  
  if (dataFreq=="Month of the year")            {tick<-31}
  if (dataFreq=="Day of the week"  )            {tick<-1}
  
  tindex <-(c(1:Number_PointsToEstimate)+ length(RecordedTravelTime))*tick ## to to adjust the tick jump Done!
  TimeStampInFuture <<-as.Date(tindex, StartDate)
  if (dataFreq=="Month of the year")            {TimeStampInFuture<-format((TimeStampInFuture-tick), format="%B %Y")}
  if (dataFreq=="Day of the week"  )            {TimeStampInFuture<-format((TimeStampInFuture-tick), format="%B %d %Y")}
  TimeStampInFuture = as.Date(time(pAdd), format="%m/%d/%Y") 

  
  MyEstimatedDataHWAdd <<- list(TimeStampInFuture,Estimated_ValueHWAdd,UpperLimitHWAdd,LowerLimitHWAdd)
  
  SSEHWADD<<-(sum((xhatHWAdd-RecordedTravelTime[(patternFreq+1):length(RecordedTravelTime)])^2))/ length (RecordedTravelTime[(patternFreq+1):length(RecordedTravelTime)])
  
  HWAdd_alpha<-TforecastsHWAdd$alpha
  HWAdd_beta<-TforecastsHWAdd$beta
  HWAdd_gamma<-TforecastsHWAdd$gamma
  HWA<-ets(TSeries,"AAA")
  HWAAIC<<-HWA$aic
  HWABIC<<-HWA$bic
  tfHWAdd <<- tempfile()
  
  parts = unlist(strsplit(filename, "/" ))
  fileout = parts[length(parts)]
  fileout = paste('model_outputs/volumes', fileout, sep = "")
  write.table(MyEstimatedDataHWAdd, fileout, 
              sep=',',col.names = c("Time_Stamp","Estimated_ValueHWAdd","UpperLimitHWAdd","LowerLimitHWAdd"),row.names = FALSE)
  }

files <- list.files(path="C:\\Users\\omarsuleman\\Documents\\1_DataTeam\\Tools\\Clinical-Laboratory\\pre_processed\\volumes_JL\\", pattern="*.csv", full.names=F, recursive=FALSE)
print(files)


lapply(files, function(x){
  filename = paste("pre_processed/volumes_JL/", x, sep="")
  print(filename)
  process_data(filename)
})

# 
# process_data("pre_processed/volumes_JL/V553889_N82_Central_Esoteric.csv")
# warnings()

