#----------- Install packages----------------
install.packages("dplyr")
install.packages("tidyr")
install.packages("highcharter")
install.packages("forecast")
install.packages("timeSeries")
install.packages("tseries")
install.packages("plotly")
install.packages("timeDate")
library(ggplot2)
library(highcharter)
library(tidyr)
library(dplyr)
library(forecast)
library(timeSeries)
library(tseries)
library(plotly)
library(timeDate)
###Importing the Data from Kaggle and S&P 500 compananies(wikipedia)
company_info <- read.csv('C:/Users/Mohita/OneDrive/Desktop/ALY 6015/Final Project/Data/Company_name.csv')
company_info
daily_stock_Prices<- read.csv('C:/Users/Mohita/OneDrive/Desktop/ALY 6015/Final Project/Data/prices-split-adjusted.csv')
daily_stock_Prices
Annual_balance<- read.csv('C:/Users/Mohita/OneDrive/Desktop/ALY 6015/Final Project/Data/fundamentals.csv')
Annual_balance

##################Cleaning Data##############################

#------------------------- Comapny Info------------------------------
# Splitting address column

install.packages("lubridate")
library("lubridate")
install.packages("splitstackshape")
library(splitstackshape)
company_info<- cSplit(company_info,"Location",",",fixed= FALSE)
company_info

#--------- Deleting columns from annual_balance------------

Annual_balance$'Period.Ending' <- NULL
Annual_balance $ 'X' <- NULL
Annual_balance $'Depreciation' <- NULL
Annual_balance $'Gross.Profit' <- NULL
Annual_balance $'Short.Term.Debt...Current.Portion.of.Long.Term.Debt' <- NULL
Annual_balance $'Profit.Margin' <- NULL
Annual_balance$'Period.Ending' <- NULL
head(Annual_balance)

#------- Renaming the columns--------
names(company_info) <- c("Ticker_Symbol","Company_Name","Sector","CIK","City","State")
names(daily_stock_Prices) <- c("date","Ticker_Symbol","Open_Price","Close_Price","Lowest_Rate","Highest_Rate","Volume")
head(daily_stock_Prices)
names(Annual_balance) <- c("Date","Ticker_Symbol","Cash_and_Eqivalent","Operating_Margin","Quick_Ratio",
                           "Retained_Earning",
                           "Short_Term_Investments","Total_Current_Asset",
                           "Total_Current_Liablities","Total_Revenue","Tresury_Stock")
head(Annual_balance)

############## Replace missing values$##############

Annual_balance$Date[is.na(Annual_balance$Date)] <- "1999"
head(Annual_balance)
daily_stock_Prices$date[is.na(daily_stock_Prices$date)] <- "1999-01-01"
head(daily_stock_Prices)

#### assignining a common name ###########
company_info$'Sector'[company_info$'Sector'=="IT"] <- "Information Technology"
company_info

#----------------- Dividing date in Day, month, year and weekday----------------
daily_stock_info_table<- daily_stock_Prices
daily_stock_Prices$date<- as.Date(daily_stock_Prices$date)
daily_stock_Prices$Day<- factor(day(as.POSIXlt(daily_stock_Prices$date)))
daily_stock_Prices$Month<- factor(month(as.POSIXlt(daily_stock_Prices$date)))
daily_stock_Prices$Year<- factor(year(as.POSIXlt(daily_stock_Prices$date)))
daily_stock_Prices$Weekday<- factor(wday(as.POSIXlt(daily_stock_Prices$date)))
head(daily_stock_Prices)
#-------------- Storing data by year, month, day and weekday---------------------------
Volume_year <-daily_stock_Prices %>% filter(Year %in% c("2014","2015","2016"))%>% group_by(Year)%>% summarize(Total =n())
Volume_month <-daily_stock_Prices %>% group_by(Month)%>% summarize(Total =n())
Month<- c("jan","feb","march","april","may","june","july","august","sept","oct","nov","dec")
Volume_month$Month <- Month[Volume_month$Month]
head(Volume_month)
head(Volume_year)
Volume_YearMonth<-daily_stock_Prices %>% 
  group_by(Year, Month) %>% 
  summarise(Total = n())
Volume_YearMonth
Volume_weekday <- daily_stock_Prices %>% group_by(Weekday)%>% summarize(Total =n())
Weekday <-c("mon","tue","wed","thur","fri")
Volume_weekday$Weekday <- Weekday[Volume_weekday$Weekday]
head(Volume_weekday)
Volume_day <- daily_stock_Prices%>% group_by(Day)%>% summarize(total =n())
head(Volume_day)

#-------------- Data Storage---------------------------
# Creating database connection
install.packages("RSQLite")
library("RSQLite")
db_conn<-dbConnect(SQLite(),dbname="Stock_Market_Analysis.sqlite")

#Creating tables in the database
#---------For company_info---------------
dbGetQuery(db_conn,# DB Connector name
           "create table Company_Info
           (
           Ticker_Symbol Text Primary Key,
           Company_Name Text,
           Sector Text,
           CIK Text,
           City Text,
           State Text)") #Create Table Script
dbWriteTable(conn=db_conn,
             name="Company_Info", #Table Name
             value=company_info, #Load data from the newly created data frame
             append=TRUE, row.names=FALSE,header=TRUE) #Since the CSV contains headers in the columns
dbListFields(db_conn,"Company_Info")#Listing the field of the table

#---------daily_stock_Prices---------------
dbGetQuery(db_conn,# DB Connector name
           "create table Daily_Stock_Info
           (
           Daily_Stock_Id Integer Primary Key Autoincrement,
           date Date,
           Ticker_Symbol Text,
           Open_Price Numeric,
           Close_Price Numeric,
           Lowest_Rate Numeric,
           Highest_Rate Numeric,
           Volume Real)") #Create Table Script
dbWriteTable(conn=db_conn,
             name="Daily_Stock_Info", #Table Name
             value=daily_stock_Prices, #Load data from the newly created data frame
             append=TRUE, row.names=FALSE,header=TRUE) #Since the CSV contains headers in the columns
dbListFields(db_conn,"Daily_Stock_Info")#Listing the field of the table

#---------Annual_balance---------------
dbGetQuery(db_conn,# DB Connector name
           "create table Annual_Balance_Information
           (
           Annual_Stock_Id Integer Primary Key Autoincrement,
           Date Integer,
           Ticker_Symbol Text,
           Cash_and_Eqivalent Numeric,
           Operating_Margin Numeric,
           Quick_Ratio Numeric,
           Retained_Earning Numeric,
           Short_Term_Investments Numeric,
           Total_Current_Asset Numeric,
           Total_Current_Liablities Numeric,
           Total_Revenue Numeric,
           Tresury_Stock Numeric)") #Create Table Script
dbWriteTable(conn=db_conn,
             name="Annual_Balance_Information", #Table Name
             value=Annual_balance, #Load data from the newly created data frame
             append=TRUE, row.names=FALSE,header=TRUE) #Since the CSV contains headers in the columns
dbListFields(db_conn,"Annual_Balance_Information")#Listing the field of the table

#-----------------Calculation--------------
#Calculating current ratio of all stocks
Annual_Info_Current_Ratio<-Annual_balance%>%
                            select(Date,Ticker_Symbol,Total_Current_Liablities,Total_Current_Asset)
Annual_Info_Current_Ratio$Current_Ratio<-Annual_Info_Current_Ratio$Total_Current_Asset/Annual_Info_Current_Ratio$Total_Current_Liablities
Annual_Profit<-filter(Annual_Info_Current_Ratio,Current_Ratio>=1) #Contains all companies having higher assets than liabilities
Annual_Loss<-filter(Annual_Info_Current_Ratio,Current_Ratio<1) #Contains all companies having higher liabilities than assets
#Calculating the cash ratio for all the stocks
Annual_Info_Cash_Ratio<-Annual_balance$Cash_and_Eqivalent/Annual_balance$Total_Current_Liablities

#-----------Display the Companies having a current ratio greater than one-----------
query<-"select a.Ticker_Symbol, c.Company_Name,(a.Total_Current_Asset/a.Total_Current_Liablities) as Current_Ratio
        from Annual_Balance_Information a
        join Company_Info c on a.Ticker_Symbol=c.Ticker_Symbol
        group by a.Ticker_Symbol, c.Company_Name
        having (a.Total_Current_Asset/a.Total_Current_Liablities)>1
        order by (a.Total_Current_Asset/a.Total_Current_Liablities) desc
        limit 10"
dbGetQuery(db_conn,query)

#-------------state having highest number of headquarters--------------
queryhdq<-"select State,count(Ticker_Symbol) as No_of_Headquarters
        from Company_Info
        group by State
        order by count(Ticker_Symbol) desc
        limit 5"
dbGetQuery(db_conn,queryhdq)

#------------------------- Display all the GICS Sector along with their Total revenue-----------
queryGICS<-"select c.Sector, sum(a.Total_Revenue) as Total_Revenue
        from Annual_Balance_Information a
        join Company_Info c on a.Ticker_Symbol=c.Ticker_Symbol
        group by  c.Sector
        order by sum(a.Total_Revenue) desc"
dbGetQuery(db_conn,queryGICS)

#-------Display the Retained earnings for Industries----------------
queryRet<-"select c.Sector, sum(a.Retained_Earning) as Total_Retained_Earnings
        from Annual_Balance_Information a
        join Company_Info c on a.Ticker_Symbol=c.Ticker_Symbol
        group by c.Sector
        order by sum(a.Retained_Earning) desc"
dbGetQuery(db_conn,queryRet)

#------------------------Data Analysis----------------------



#1) Plot a graph of how much a company make's on each dollar of sales [Operation Margin]
Opt_Margin <- Annual_balance %>%
  select(Ticker_Symbol,Operating_Margin) %>%
  arrange(desc(Operating_Margin)) %>%
  head(n=10) %>%
  distinct()

hchart(Opt_Margin, 'column', hcaes(x = Ticker_Symbol, y = Operating_Margin, color = Operating_Margin)) %>%
  hc_add_theme(hc_theme_flat()) %>%
  hc_title(text = "Top 10 stocks with highest Operation Margin")

#2)Display the number of headquarters according to State.
State_HQ <- company_info %>%
  select(State) %>%
  group_by(State) %>%
  summarise(No_of_HQ = n()) %>%
  arrange(desc(No_of_HQ))

hchart(State_HQ[1:2], 'pie', hcaes (x = State, y = No_of_HQ, color = No_of_HQ)) %>%
  hc_add_theme(hc_theme_google()) %>%
  hc_title(text = "Number of headquarters according to State")


#3)Volume of Stocks traded by Month
hchart(Volume_month, 'line', hcaes(x = Month, y = Total, color = Total)) %>%
  hc_title(text = "Volume of Stocks traded by month of the year")

#4)Volume of Stocks traded by each day of the month

hchart(Volume_day, 'column', hcaes(x = Day, y = total, color  = total)) %>%
  hc_title(text = "Volume of Stocks traded by each Day of the month")

#5)Volume of Stocks traded by Weekday
hchart(Volume_weekday, 'line', hcaes(x = Weekday, y = Total, color = Total)) %>%
  hc_title(text = "Volume of Stocks traded by Day of the Week")

#6) Which Companies have short term investments due compared to Cash and Cash Equivalents
query <- "Select Ticker_Symbol, Short_Term_Investments, Cash_and_Eqivalent, Short_Term_Investments - Cash_and_Eqivalent
from Annual_Balance_Information
group by Ticker_Symbol
having Short_Term_Investments > Cash_and_Eqivalent and Short_Term_Investments > 0 and Cash_and_Eqivalent > 0 
order by (Short_Term_Investments - Cash_and_Eqivalent) desc
limit 10"

Loss_Stock <- dbGetQuery(db_conn, query)

plot_ly(Loss_Stock, x = Loss_Stock$Ticker_Symbol, y = Loss_Stock$Short_Term_Investments, type = 'bar', name = 'Short Term Investments Due')
add_trace(y  = Loss_Stock$Cash_and_Equivalent, name  = 'Cash and Cash Equivalent') %>%
  layout(yaxis = list(title = 'Amount in Dollars'), barmode = 'group', title='Companies with higher Short Term Investments Due compared to Cash and Cash Equivalents')

#------------------------ARIMA----------------------------
Volume_data_arima<- daily_stock_Prices %>% group_by(date) %>% summarize(Total = n())

#Create Time series
Volume_Date_ARIMA = ts(na.omit(Volume_data_arima$Total), start=c(2015,1), end=c(2018,12),frequency=24) #Preparing time series
plot(Volume_Date_ARIMA)

#Decomposing the data

Decomp = stl(Volume_Date_ARIMA, s.window="periodic") #STL is a flexible function for decomposing and forecasting the series. 
Deseasonal<-seasadj(Decomp) #Returns seasonally adjusted data constructed by removing the seasonal component.
plot(Decomp)

# To check if mean is stationary
adf.test(Volume_Date_ARIMA,alternative="stationary")
# p-value is greater than 0.05 hence we can say that mean is not constant

#Fitting the model

Arima=auto.arima(Volume_Date_ARIMA,trace = TRUE,test="kpss",ic="aic") 
plot.ts(Arima$residuals)
acf(Arima$residuals,lag.max=54)
pacf(Arima$residuals,lag.max=54)

####Forecast for 24 months

Arima_Forecast = forecast(Arima, h=24)
Arima_Forecast
plot(Arima_Forecast, xlab="Time") #Plots the forecast graph for the best model selected above by Auto_ARIMA
