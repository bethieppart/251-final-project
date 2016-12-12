require("ggplot2")

df1 = read.csv("../data/CombinedOutput_2015_12-2016_4.csv", header = FALSE)
df2 = read.csv("../data/CombinedOutput_20160831_20161209.csv", header = FALSE)
df3 = read.csv("../data/CombinedOutputAnthony.csv", header = FALSE)
df4 = read.csv("../data/CombinedOutputBeth.csv", header = FALSE)

df <- rbind(df1,df2,df3,df4)

colnames(df)<-c("date", "location", "label", "tweetCount", "avgPolarity", "totalSentiment","avgSentiment","timestamp")
df$location <- sapply(df$location, tolower)
names(df$location)<-NULL
df$date<-substr(gsub("[^0-9/-]","", df$date), 0, 10)
stateNames<-sapply(state.name, tolower)
names(stateNames)<-NULL

A<-df[df$location %in% c("united states", stateNames),]
A$date<-as.Date(A$date)
by(A$location, A$location, length)
by(A$location, A$label, length)

pdf("TwitterElection.pdf")
ggplot(A[A$label!="political",], aes(x=label, y=tweetCount, fill=label)) + stat_summary(fun.y="sum", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Tweet Counts By Topic") + ylab("Tweet Count")
ggplot(A, aes(x=label, y=avgPolarity, fill=label)) + stat_summary(fun.y="mean", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Polarity By Topic") + ylab("Tweet Count")
ggplot(A, aes(x=label, y=avgSentiment, fill=label)) + stat_summary(fun.y="mean", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Sentiment By Topic") + ylab("Tweet Count")

#Raw Counts
ggplot(A[A$location=="united states" & A$label!="political",]) + scale_x_date(labels = date_format("%m-%Y")) + geom_line(aes(x=date, y=tweetCount, group=label, color=label))+theme(axis.text.x = element_text(angle = 90)) + ggtitle("Tweet Count Time Series") + ylab("Count")

#Polarity
ggplot(A[A$location=="united states",],aes(x=date, y=avgPolarity, group=label,color=label))+ scale_x_date(labels = date_format("%m-%Y"))  + geom_smooth(aes(group=label,color=label)) +  stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Polarity Time Series")
ggplot(A[A$location=="united states",],aes(x=date, y=avgPolarity, group=label,color=label))+ scale_x_date(labels = date_format("%m-%Y"))  + geom_smooth(aes(group=label,color=label)) +  stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + facet_wrap(~label) + ggtitle("Average Polarity Time Series")

#Sentiment
ggplot(A[A$location=="united states",],aes(x=date, y=avgSentiment, group=label,color=label))+ scale_x_date(labels = date_format("%m-%Y"))  + geom_smooth(aes(group=label,color=label))  + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90))+ ggtitle("Average Sentiment Time Series")
ggplot(A[A$location=="united states",],aes(x=date, y=avgSentiment, group=label,color=label))+ scale_x_date(labels = date_format("%m-%Y"))  + geom_smooth(aes(group=label,color=label)) + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + facet_wrap(~label)+ ggtitle("Average Sentiment Time Series")

#Need to get 538 polling data, then splice it into my time series charts.  
#Maps would be nice, but seems like something more for tableau or another teammate to do

dev.off()

# this next code will need to be adapted if you don't have a ../data/ folder...
# alternatively, if that link stops working, there's a static copy at http://ellisp.github.io/data/polls.csv
www <- "http://projects.fivethirtyeight.com/general-model/president_general_polls_2016.csv"
download.file(www, destfile = "../data/polls.csv")

# download data
plz <- read.csv("../data/polls.csv", stringsAsFactors = FALSE)
plz$url<-NULL
plz<-plz[plz$state=='U.S.',]
plz$date<-as.Date(strptime(plz$createddate, '%m/%d/%y'))

trmp<-do.call(rbind.data.frame, by(plz$rawpoll_trump, plz$date, mean, simplify = FALSE))
clint<-do.call(rbind.data.frame, by(plz$rawpoll_clinton, plz$date, mean, simplify = FALSE))
johns<-do.call(rbind.data.frame, by(plz$rawpoll_johnson, plz$date, mean, simplify = FALSE))
names(trmp)<-c('trump_avg')
names(clint)<-c('clint_avg')
names(johns)<-c('johns_avg')
pollAvg<-data.frame(date=row.names(trmp), trump_avg=trmp$trump_avg,clint_avg=clint$clint_avg,johns_avg=johns$johns_avg)


ggplot(pollAvg, aes(x=as.Date(date))) + geom_point(aes(y=clint_avg, color="Clinton"), color="Blue") + geom_point(aes(y=trump_avg, color="Trump"), color="Red") +
  scale_x_date(labels = date_format("%m-%Y")) + ylim(25,75)



ggplot(pollAvg, aes(x=as.Date(date))) + geom_point(aes(y=clint_avg, color="Clinton")) + geom_smooth(aes(y=clint_avg, color="Clinton")) + geom_point(aes(y=trump_avg, color="Trump")) + geom_smooth(aes(y=trump_avg, color="Trump")) +
  scale_x_date(labels = date_format("%m-%Y")) + ylim(25,75)+scale_color_manual(name = "Candidate", 
                   labels = c("Clinton", "Trump"),
                   values = c("Blue","Red")) + xlab("Date") + ylab("Polling Average") + ggtitle("National Election Polling Averages")

