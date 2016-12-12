require("ggplot2")

df = read.csv("../../CombinedOutput.csv", header = FALSE)
colnames(df)<-c("date", "location", "label", "tweetCount", "avgPolarity", "totalSentiment","avgSentiment","timestamp")
df$location <- sapply(df$location, tolower)
names(df$location)<-NULL
df$date<-substr(gsub("[^0-9/-]","", df$date), 0, 10)
stateNames<-sapply(state.name, tolower)
names(stateNames)<-NULL

A<-df[df$location %in% c("united states", stateNames),]

by(A$location, A$location, length)
by(A$location, A$label, length)

pdf("TwitterElection.pdf")
ggplot(A, aes(x=label, y=tweetCount, fill=label)) + stat_summary(fun.y="sum", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Tweet Counts By Topic") + ylab("Tweet Count")
ggplot(A, aes(x=label, y=avgPolarity, fill=label)) + stat_summary(fun.y="mean", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Polarity By Topic") + ylab("Tweet Count")
ggplot(A, aes(x=label, y=avgSentiment, fill=label)) + stat_summary(fun.y="mean", geom="bar") +theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Sentiment By Topic") + ylab("Tweet Count")

#Raw Counts
ggplot(A[A$location=="united states",]) + geom_line(aes(x=date, y=tweetCount, group=label, color=label))+theme(axis.text.x = element_text(angle = 90)) + ggtitle("Tweet Count Time Series") + ylab("Count")

#Polarity
ggplot(A[A$location=="united states",],aes(x=date, y=avgPolarity, group=label,color=label)) + geom_smooth(aes(group=label,color=label)) + geom_line(aes(group=label,color=label), size=1) + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + ggtitle("Average Polarity Time Series")
ggplot(A[A$location=="united states",],aes(x=date, y=avgPolarity, group=label,color=label)) + geom_smooth(aes(group=label,color=label)) + geom_line(aes(group=label,color=label), size=1) + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + facet_wrap(~label) + ggtitle("Average Polarity Time Series")

#Sentiment
ggplot(A[A$location=="united states",],aes(x=date, y=avgSentiment, group=label,color=label)) + geom_smooth(aes(group=label,color=label)) + geom_line(aes(group=label,color=label), size=1) + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90))+ ggtitle("Average Sentiment Time Series")
ggplot(A[A$location=="united states",],aes(x=date, y=avgSentiment, group=label,color=label)) + geom_smooth(aes(group=label,color=label)) + geom_line(aes(group=label,color=label), size=1) + stat_summary(fun.y="mean", geom="point")+theme(axis.text.x = element_text(angle = 90)) + facet_wrap(~label)+ ggtitle("Average Sentiment Time Series")

#Need to get 538 polling data, then splice it into my time series charts.  
#Maps would be nice, but seems like something more for tableau or another teammate to do

dev.off()
