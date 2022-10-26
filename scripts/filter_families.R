#load libraries----
library(groundhog)
my.date <- "2022-10-16"
pkgs <- c("tidyverse","ggplot2")
groundhog.library(pkgs,my.date)



#set working directory----
#setwd(getSrcDirectory()[1]) # run this line if using R
setwd(dirname(rstudioapi::getActiveDocumentContext()$path)) #run this line if using RStudio
setwd('..')


#load data----
forms <- read.csv("cldf/forms.csv")
languages <- read.csv("cldf/languages.csv", na.strings=c("","NA"))
parameters <- read.csv("cldf/parameters.csv")

#convert categorical variables to factor
new_factors <- c("Glottocode","Glottolog_Name","ISO639P3code","Macroarea","Family","BestBase","Base")
languages[,new_factors] <- lapply(languages[,new_factors] ,factor)


#String manipulation --this can be improved with lingpy----

#Define normalized Levenshtein distances

na.lev <- function(str1, str2) {
  norm <- max(nchar(str1), nchar(str2))
  ln <- adist(str1, str2)/norm
  return(ln)
}

#Eliminate diacritics

simp.forms <- forms
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"ˈ","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"ː","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"ʔ","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"ʰ","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"̥","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"ˌ","")
simp.forms$Form <- sapply(simp.forms$Form, str_replace_all,"̩","")

#Keep only complete cases with numerals 1-30
#intermediate: char to numeric in Parameter_ID --check for errors above 100 (I'm not using them here) 
# --- this can be extracted directly from the dataset merging
simp.forms %>%
  distinct(Parameter_ID) -> chrtonumeric


chrtonumeric$Numeric_ID <- c(1:100,
                             1000,1000000,1000000000,200,2000,400,800,111,900,4000,
                             9000,120,300,103,500,600,700,101,3000,210,
                             10000,150,1060,5000,100000,999,10000000,240,1001,6000,
                             7000,8000,144,133,140,201,102,123,250,2000000,
                             110,231,1221,202,1002,1060,1200,399,2002,108,
                             104,3041027,0,100000000,10000000000,100000000000,1000000000000)

simp.forms <- merge(simp.forms,chrtonumeric,by="Parameter_ID")

simp.forms %>%
  group_by(Language_ID) %>%
  filter(Numeric_ID <= 30) %>% #keep only numerals 1-30
  distinct(Numeric_ID, .keep_all = TRUE) %>% #keep only one form per number --check that it's the best form!
  filter(n()==30) -> tmp #keep only complete cases

my.df <- merge(tmp,languages,by.x="Language_ID",by.y="ID")


my.df.short <- select(my.df,c(ID,Name,Language_ID,Macroarea,Family,Parameter_ID,Numeric_ID,Form,Base,BestBase))

all.languages <- unique(my.df.short$Language_ID)




#Length of numerals----

#we choose all languages in the dataset
my.languages <- unique(my.df$Language_ID)

# we calculate the length of each numeral string, and then its rank in its numeral system

i<-1
the.lengths <- NULL
the.ranks <- NULL
ranks.as.strings <- NULL

we.keep <- c(1:30) #numerals from 1 to 30

for(language in my.languages){
  df <- filter(my.df.short,Language_ID==language)
  the.numbers <- df$Form[order(df$Numeric_ID)]
  if(i==1){
    the.lengths <- as.data.frame(t(nchar(the.numbers)))
    the.ranks <- as.data.frame(t( rank(the.lengths,ties.method = "min") ))
    ranks.as.strings <- as.data.frame(paste(the.ranks,collapse=" ")) }
  else{the.lengths[i,] <- nchar(the.numbers)
  the.ranks[i,] <- rank(the.lengths[i,],ties.method = "min")
  ranks.as.strings[i,] <- paste(the.ranks[i,],collapse=" ")
  }
  rownames(the.lengths)[i] <- language
  rownames(the.ranks)[i] <- language
  i <- i+1
}
colnames(the.lengths) <- we.keep
colnames(the.ranks) <- we.keep

the.lengths <- the.lengths[sort(my.languages,decreasing=F),]



#Global study

my.languages <- unique(my.df$Language_ID)


#we calculate the ranks. If two numerals have the same length(e.g. UAN (3), TU (2), TRI (3), FOUR(4)), 
#we can make a tied rank (2,1,2,4) or a random one [either (2,1,3,4) or (3,1,2,4), randomly]
#Cumulative probability for N means for how many numerals are shorter or equal than N

i<-1
the.lengths <- NULL
the.ranks.tied <- NULL
the.ranks.random <- NULL
ranks.as.strings.tied <- NULL
cumulative.probability <- NULL

for(language in my.languages){
  df <- filter(my.df.short,Language_ID==language)
  the.numbers <- df$Form[order(df$Numeric_ID)]
  if(i==1){
    the.lengths <- as.data.frame(t(nchar(the.numbers))) 
    the.ranks.tied <- as.data.frame(t( rank(the.lengths,ties.method = "min") ))
    the.ranks.random <- as.data.frame(t( rank(the.lengths,ties.method = "random") ))
    ranks.as.strings.tied <- as.data.frame(paste(the.ranks.tied,collapse=" ")) } 
  else{the.lengths[i,] <- nchar(the.numbers)
  the.ranks.tied[i,] <- rank(the.lengths[i,],ties.method = "min")
  the.ranks.random[i,] <- rank(the.lengths[i,],ties.method = "random")
  ranks.as.strings.tied[i,] <- paste(the.ranks.tied[i,],collapse=" ") 
  }
  rownames(the.lengths)[i] <- language
  rownames(the.ranks.tied)[i] <- language
  rownames(the.ranks.random)[i] <- language
  i <- i+1  
}
colnames(the.lengths) <- we.keep
colnames(the.ranks.tied) <- we.keep
colnames(the.ranks.random) <- we.keep

the.lengths <- the.lengths[sort(my.languages,decreasing=F),]



##cumulative probability
cumulative.probability <- the.ranks.tied
for(j in nrow(cumulative.probability)){
  this <- as.numeric(the.ranks.tied[j,])
  #try>this
  for(i in 1:30){
    cumulative.probability[j,i] <- sum(this <= this[i])
  }
}



#k means-----
library(ggpubr)
library(factoextra)


#explore the optimal k = number of clusters

p<-list(0)
elbows <- list(0)
for(k in 1:20){
  res.km.cumulative <-kmeans(cumulative.probability,k)
  res.km.cumulative$cluster
  p[[k]] <- fviz_cluster(res.km.cumulative, data = cumulative.probability,
               # palette = c("#2E9FDF", "#00AFBB", "#E7B800"), 
               geom = "point",
               ellipse.type = "convex", 
               ggtheme = theme_bw()
  )
  elbows[[k]] <- res.km.cumulative$tot.withinss
}
plot(unlist(elbows), type="b")

#the elbow seems to be in k=4

k<-4 #optimal k

res.km.cumulative <-kmeans(cumulative.probability,k)
p <- fviz_cluster(res.km.cumulative, data = cumulative.probability,
                       # palette = c("#2E9FDF", "#00AFBB", "#E7B800"), 
                       geom = "point",
                       ellipse.type = "convex", 
                       ggtheme = theme_bw() )

p


write.csv(as.data.frame(res.km.cumulative$centers),"results/clusters.k3.csv")

table(res.km.cumulative$cluster) #languages in each cluster
cluster <- res.km.cumulative$cluster

rownames_to_column(cumulative.probability,var="Language_ID") %>%
  cbind(.,cluster) %>%
  right_join(., my.df.short, by="Language_ID") %>%
  select(Language_ID,Macroarea,Family,Base,cluster) %>%
  distinct(Language_ID,Macroarea,Family,Base,cluster, .keep_all = TRUE) ->this


#we can plot per macroarea and family, provided this information is imported

#plotting -----

the.clusters <- tibble::rownames_to_column(as.data.frame(res.km.cumulative$centers), "cluster")

#30 langs
lendf<-the.clusters  %>%
  pivot_longer(cols=c(2:last_col()),names_to = "Numeral") %>%
  filter(Numeral!="cluster")
lendf$Numeral<-as.numeric(gsub("cluster","",lendf$Numeral))

# Plot
lendf %>%
  ggplot(aes(x=Numeral,y=value/30))+
  geom_line(size=2)+
  geom_point(size=3)+
  facet_wrap(~cluster,nrow=4)+
  theme_minimal()+
  scale_y_continuous(limits=c(0,1),breaks=c(0,0.5,1),expand=c(0,0))+
  scale_x_continuous(breaks=c(1,5,10,15,20,15,30),expand=c(0.05,0.05))+
  labs(y="Cumulative probability")+
  theme(plot.background = element_rect(fill="white"),axis.text=element_text(size=15),axis.title = element_text(size=15))

ggsave("figures/numeralbank_prototypes.30.png")
table(res.km.cumulative$cluster)



