################################################################################
# This script is our MGMT 474 Predictive Analytics Project script. We are pulling
# data from my MySQL using the RMariaDB library
#
# Data dictionary: see Dropbox folder
################################################################################
options(java.parameters = "-Xmx64048m") # 64048 is 64 GB
#install.packages("odbc")
#install.packages("RMariaDB")
library(RMariaDB)
# Connect to a MariaDB version of a MySQL database
con <- dbConnect(RMariaDB::MariaDB(), host="datamine.rcac.purdue.edu", port=3306
                 , dbname="Perceivant"
                 , user="Perceivant_user", password="Perceivant2019")

# list of db tables
dbListTables(con)

# query tables
grades1 <- dbGetQuery(con, "SELECT * FROM Course_Grades_DetailView_Fall_2018")
grades2 <- dbGetQuery(con, "SELECT * FROM Course_Grades_FinalGradeOnlyView_Fall_2018")
pageviews <- dbGetQuery(con, "SELECT * FROM KSU_Assessment_PageViews_Fall_2018")
clicks <- dbGetQuery(con, "SELECT * FROM KSU_Click_History_Fall_2018")
log_2 = dbGetQuery(con, "SELECT * FROM KSU_Assessment_Response_Log_Fall_2018_allrows_duplicate")
test <- dbGetQuery(con, "SELECT * FROM F2FTests")
online <- dbGetQuery(con, "SELECT * FROM Online")

# close db connection
suppressWarnings(dbDisconnect(con)); rm(con)



############################Data Cleaning#########################################
library(plyr)
library(dplyr)
library(caret)
library(tidyr)


#########################################processing grades1
#removing and converting columns og grades 1
str(grades1)
grades1$index = NULL
#grades1[ ,c(4:8)] = NULL
grades1$Instructor = as.factor(grades1$Instructor)
grades1[4:82] <- lapply(grades1[4:82], as.numeric)
grades1 = subset(grades1, grades1['Quiz 1: Chapter 1-3'] >= 0 )
colnames(grades1)[colnames(grades1) == "Possilbe Points" ] = "Possible Points"
summary(grades1)

#create new column to identify lecture type
grades1$lectureType = ifelse(grepl("/W", grades1$Section), "Web",
                             ifelse(grepl("/H", grades1$Section), "Hybrid", "Normal"))
grades1$lectureType = as.factor(grades1$lectureType)

#create new column that sums up total guided learning score
grades1$total_guide_score =  rowSums(grades1[,c(9:22)], na.rm = TRUE)
#create new column that sums up total quiz score for each student
grades1$total_quiz_score = rowSums(grades1[,c(23:33)],na.rm = TRUE)
#create new column that sums up total assessment score 24:48
grades1$total_assess_score = rowSums(grades1[,c(34:58)],na.rm = TRUE)
#create new column that sums up total physical challenge score
grades1$total_phy_challenge = rowSums(grades1[,c(59:62)])
#create new column that sums up total project score
grades1$total_proj_score = rowSums(grades1[,c(63:68)], na.rm = TRUE)
#create new column that sums up total rando assignment score
grades1$total_rando_score = rowSums(grades1[ ,c(69:82)], na.rm = TRUE)

new_grades = grades1[ ,c(1:8,79:ncol(grades1))]
str(new_grades)

########################################processing online grades
str(online)

online$index = NULL
online$`Unnamed: 71` = NULL
online$Instructor = as.factor(online$Instructor)
#online[ ,c(4:8)] = NULL
online[4:ncol(online)] <- lapply(online[4:ncol(online)], as.numeric)
colnames(online)[colnames(online) == "Possilbe Points" ] = "Possible Points"
#create new column to identify lecture type
online$lectureType = ifelse(grepl("/W", online$Section), "Web",
                            ifelse(grepl("/H", online$Section), "Hybrid", "Normal"))
online$lectureType = as.factor(online$lectureType)


#create new column that sums up total guided learning score
online$total_guide_score =  rowSums(online[,c(9:22)], na.rm = TRUE)
#create new column that sums up total quiz score for each student
online$total_quiz_score = rowSums(online[,c(23:28)],na.rm = TRUE)
#create new column that sums up total assessment score 24:48
online$total_assess_score = rowSums(online[,c(29:53)],na.rm = TRUE)
#create new column that sums up total physical challenge score
online$total_phy_challenge = rowSums(online[,c(54:57)])
#create new column that sums up total project score
online$total_proj_score = rowSums(online[,c(58:63)], na.rm = TRUE)
#create new column that sums up total Discussioin score
online$total_diss_score = rowSums(online[ ,c(64:66)], na.rm = TRUE)
#since it only has one rando score, change name to total rando score                                
colnames(online)[colnames(online) == "Rando Assignment 1" ] = "total_rando_score"

new_online_grades = online[ , c(1:8,67:78)]
str(new_online_grades)
summary(new_online_grades)
summary(new_grades)
summary(new_test)

#####################################processing F2Ftest dataset
str(test)
test$index = NULL
test$`Unnamed: 78` = NULL
test$Instructor = as.factor(test$Instructor)
#test[ ,c(4:8)] = NULL
test[4:ncol(test)] <- lapply(test[4:ncol(test)], as.numeric)

#create new column that sums up total guided learning score
test$total_guide_score =  rowSums(test[,c(9:22)], na.rm = TRUE)

#create new column that sums up total assessment score 24:48
test$total_assess_score = rowSums(test[,c(25:49)],na.rm = TRUE)
#create new column that sums up total physical challenge score
test$total_phy_challenge = rowSums(test[,c(50:53)])
#create new column that sums up total project score
test$total_proj_score = rowSums(test[,c(54:59)], na.rm = TRUE)
#create new column that sums up Rando Assignment score
test$total_rando_score = rowSums(test[ ,c(60:73)], na.rm = TRUE)


new_test = test[ , c(1:8,23:24,74:ncol(test))]
str(new_test)
new_test$lectureType = ifelse(grepl("/W", test$Section), "Web",
                              ifelse(grepl("/H", test$Section), "Hybrid", "Normal"))
new_test$lectureType = as.factor(new_test$lectureType)
###############combine datasets
combined_grades = rbind.fill(new_grades,new_online_grades,new_test)
str(combined_grades)
length(unique(combined_grades$User_param_external_user_id))
summary(combined_grades)

combined_grades$Attendence = NULL
combined_grades$Section = NULL
str(combined_grades)




##############################################need to add total time each spent on reading
str(pageviews)

pageviews$index = NULL
pageTime = aggregate( duration ~ user_param_external_user_id, pageviews, sum)
pageTime$duration = pageTime$duration / 1000 / 60
colnames(pageTime)[colnames(pageTime) == 'duration'] ='duration(minutes)'
colnames(pageTime)[colnames(pageTime) == 'user_param_external_user_id'] ='User_param_external_user_id'
combined_grades2<- merge(combined_grades, pageTime,by="User_param_external_user_id")
############################################################
#write.csv(file="combinedGrades.csv", x=combined_grades2)


####################process Assessment response log table
log2<-log_2[,c(14,2:13,15:22)]
view<-pageviews[,c(14,2:13)]

#str(log2)
log2[,c(2,3,6,8,11,13:21)]<-NULL



log2$correct<-ifelse(log2$score == log2$correct_threshold,1,0)
log2$total<-1
log2$question_type = as.factor(log2$question_type)
log2[,c(2:4,6)]<-NULL
#unique(log2$question_type)

log_agg<-aggregate(.~ user_param_external_user_id+question_type+taxonomy,log2,sum)  #group by user, question type, taxonomy
log_agg$acc_rate<-log_agg$correct/log_agg$total

log0<-unite(log_agg,newcol,c(question_type,taxonomy),remove = TRUE)
log0[,c(3,4)]<-NULL
#str(log0)
log0$newcol<-as.factor(log0$newcol)

#####################


#################### new df to save acc
acc<-data.frame(matrix(ncol=15,nrow=length(unique(log0$user_param_external_user_id))))
names(acc)<-c('id','matching_1','matching_2','matching_3','matching_4','multipleChoice_1','multipleChoice_2','multipleChoice_3','multipleChoice_4','multipleChoice_5','multipleSelect_1',
              'multipleSelect_2','multipleSelect_3','multipleSelect_4','multipleSelect_5')

acc$id<-unique(log0$user_param_external_user_id)

for (i in log0$user_param_external_user_id){
  acc[acc$id==i,]$matching_1<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='matching_1',]$acc_rate),
                                     log0[log0$user_param_external_user_id==i & log0$newcol=='matching_1',]$acc_rate,0)
  acc[acc$id==i,]$matching_2<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='matching_2',]$acc_rate),
                                     log0[log0$user_param_external_user_id==i & log0$newcol=='matching_2',]$acc_rate,0)
  acc[acc$id==i,]$matching_3<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='matching_3',]$acc_rate),
                                     log0[log0$user_param_external_user_id==i & log0$newcol=='matching_3',]$acc_rate,0)
  acc[acc$id==i,]$matching_4<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='matching_4',]$acc_rate),
                                     log0[log0$user_param_external_user_id==i & log0$newcol=='matching_4',]$acc_rate,0)
  acc[acc$id==i,]$multipleChoice_1<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_1',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_1',]$acc_rate,0)
  acc[acc$id==i,]$multipleChoice_2<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_2',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_2',]$acc_rate,0)
  acc[acc$id==i,]$multipleChoice_3<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_3',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_3',]$acc_rate,0)
  acc[acc$id==i,]$multipleChoice_4<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_4',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_4',]$acc_rate,0)
  acc[acc$id==i,]$multipleChoice_5<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_5',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleChoice_5',]$acc_rate,0)
  acc[acc$id==i,]$multipleSelect_1<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_1',]$acc_rate),
                                            log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_1',]$acc_rate,0)
  acc[acc$id==i,]$multipleSelect_2<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_2',]$acc_rate),
                                             log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_2',]$acc_rate,0)
  acc[acc$id==i,]$multipleSelect_3<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_3',]$acc_rate),
                                             log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_3',]$acc_rate,0)
  acc[acc$id==i,]$multipleSelect_4<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_4',]$acc_rate),
                                             log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_4',]$acc_rate,0)
  acc[acc$id==i,]$multipleSelect_5<-ifelse(!is.null(log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_5',]$acc_rate),
                                             log0[log0$user_param_external_user_id==i & log0$newcol=='multipleSelect_5',]$acc_rate,0)
    
}

colnames(acc)[colnames(acc) == 'id'] ='User_param_external_user_id'
combined_grades2$User_param_external_user_id<-as.numeric(combined_grades2$User_param_external_user_id)

acc_new<-acc
acc_new[is.na(acc_new)]<-0

##acc$matching_1<-ifelse(acc$id==log0$user_param_external_user_id & log0$newcol=='matching_1' & !is.null(log0[log0$newcol=='matching_1',]$acc_rate),
                                  # log0[log0$newcol=='matching_1',]$acc_rate,0)


##############merge acc with grade

final<-merge(combined_grades2,acc,by='User_param_external_user_id',all.x = TRUE)
final$y<-ifelse(final$`Grade (%)`>=70,1,0)

#write.csv(file="final.csv", x=final)


final$Payment<-NULL
final$User_param_external_user_id<-NULL
final$`Possible Points`<-NULL
final$`Total Points Earned`<-NULL
final$`Grade (%)`<-NULL

str(final)

final[is.na(final)]<-0

################# dummy

dummies <- dummyVars(y ~ ., data = final)            # create dummies for Xs
ex <- data.frame(predict(dummies, newdata = final))  # actually creates the dummies
names(ex) <- gsub("\\.", "", names(ex))          # removes dots from col names
d <- cbind(final$y, ex)                              # combine target var with Xs
names(d)[1] <- "y"                               # name target var 'y'
rm(dummies, ex)                                  # clean environment

d$y<-as.factor(d$y)

############## high corr
descrCor <-  cor(d[,2:ncol(d)])                           # correlation matrix
highCorr <- sum(abs(descrCor[upper.tri(descrCor)]) > .80) # num Xs with cor > t
summary(descrCor[upper.tri(descrCor)])                    # summarize the cors

# which columns in your correlation matrix have a correlation greater than some
# specified asolute cutoff. Find them and remove them
highlyCorDescr <- findCorrelation(descrCor, cutoff = 0.80)
filteredDescr <- d[,2:ncol(d)][,-highlyCorDescr] # remove those specific columns
descrCor2 <- cor(filteredDescr)                  # calculate a new cor matrix
# summarize those correlations to see if all features are now within our range
summary(descrCor2[upper.tri(descrCor2)])

highlyCorDescr

# update dataset by removing those filtered vars that were highly correlated
d <- cbind(d$y, filteredDescr)
names(d)[1] <- "y"

################### linear
y <- d$y

# create a column of 1s. This will help identify all the right linear combos
d <- cbind(rep(1, nrow(d)), d[2:ncol(d)])
names(d)[1] <- "ones"

# identify the columns that are linear combos
comboInfo <- findLinearCombos(d)
comboInfo

# remove columns identified that led to linear combos
d <- d[, -comboInfo$remove]

# remove the "ones" column in the first column
d <- d[, c(2:ncol(d))]

# Add the target variable back to our data.frame
d <- cbind(y, d)

rm(y, comboInfo)  # clean up

rm(filteredDescr, descrCor, descrCor2, highCorr, highlyCorDescr)  # clean up


######################### normalize
# d set
preProcValues <- preProcess(d[,2:ncol(d)], method = c("range"))
d <- predict(preProcValues, d)

rm(preProcValues) # clean up R environment




######################### model

library(h2o)
h2o.init(nthreads=12, max_mem_size="64g")

# load data into h2o cluster
data <- as.h2o(d)

y <- "y"                                # target variable to learn
x <- setdiff(names(data), y)                # features are all other columns
parts <- h2o.splitFrame(data, 0.8, seed=99) # randomly partition data into 80/20
train <- parts[[1]]                         # random set of training obs
test <- parts[[2]]                          # random set of testing obs

library(DMwR)
tr<-as.data.frame(train)
tr_smote<-SMOTE(y~., tr, perc.over = 600,perc.under = 100)
summary(tr_smote)
train1<-as.h2o(tr_smote)



# models
rf <- h2o.randomForest(x, y, train1,nfolds=5)
dl <- h2o.deeplearning(x, y, train1,nfolds=5)
gbm <- h2o.gbm(x, y, train1,nfolds=5)
nb<-h2o.naiveBayes(x,y, train1,nfolds = 5)
glm<-h2o.glm(x,y,train1,family='binomial',nfolds=5)



h2o.performance(rf, train1)
h2o.performance(rf, test)
h2o.performance(dl, train1)
h2o.performance(dl, test)
h2o.performance(gbm, train1)
h2o.performance(gbm, test)


h2o.performance(glm, train1)
h2o.performance(glm, test)

summary(glm)









