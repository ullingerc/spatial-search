# R script to evaluate the results of the case study queries

# Load required modules
library(corrplot)  # For nice colorful correlation matrices
library(car) # "Companion to applied regression"- for variance inflation factor

# Workaround: Penalty against scientific notation ?e-? (for write.csv)
options(scipen=9999)

# Load QLever result TSV files
ew24 <- read.csv("ew24.tsv", sep="\t")
btw21 <- read.csv("btw21.tsv", sep="\t")

# Columns holding aggregated data from OSM and DELFI-GTFS
distcols <- c("X.dist_transport_avg",
              "X.dist_supermarket_avg",
              "X.dist_butcher_avg",
              "X.dist_motorway_avg",
              "X.dist_hospital_avg",
              "X.dist_university_avg",
              "X.dist_fuel_avg",
              "X.dist_gastronomy_avg",
              "X.dist_kindergarten_avg",
              "X.dist_hairdresser_avg",
              "X.dist_school_avg",
              "X.dist_bakery_avg",
              "X.dist_pharmacy_avg",
              "X.n_trips_avg")

# Correlation matrix should be built with population density and polarization
corcols <- c(distcols,
             "X.pop_density",
             "X.polarization")

# Function to generate a linear model and prepare it is a dataframe for csv
# export
makelinearmodel <- function (modelname, vars, dataset) {
  m = lm(formula = dataset$X.polarization ~ ., data = dataset[vars])
  s = summary(m)

  # Prepare export of significance levels to csv
  # Source: <https://statisticsglobe.com/extract-significance-levels-from-
  # linear-regression-model-in-r>
  mod_summary_sign = s$coefficients[ , 4]  # Pull out p-values
  mod_summary_stars = NA  # Named vector with significance stars
  mod_summary_stars[mod_summary_sign < 0.1] <- "."
  mod_summary_stars[mod_summary_sign < 0.05] <- "*"
  mod_summary_stars[mod_summary_sign < 0.01] <- "**"
  mod_summary_stars[mod_summary_sign < 0.001] <- "***"
  mod_summary_stars[is.na(mod_summary_stars)] <- " "
  names(mod_summary_stars) <- names(mod_summary_sign)

  # Prepare row and col names for csv export
  mss = data.frame(mod_summary_stars)
  mss = cbind(rownames(mss),mss)
  rownames(mss) <- NULL
  colnames(mss) <- c("variableName","significance")

  # Add coefficients and std. error
  mss$coef <- s$coefficients[,1]
  mss$err <- s$coefficients[,2]

  # Add other model information
  mss <- rbind(mss,c("QUALITYn", "", nrow(dataset)[1], ""))
  mss <- rbind(mss,c("QUALITYAdjRSquared", "", s$adj.r.squared, ""))
  mss <- rbind(mss,c("QUALITYFStatistic", "", s$fstatistic[1], ""))

  # Helper for joining
  mss$number <- row.names(mss)
  return(list("modelname" = modelname, "summary" = s, "model" = m,
              "model_table" = as.data.frame(mss)))
}

# Given all models from the evaluation, merge their tables into one data frame
# for csv export
variableName <- c(
  "X.dist_hospital_avg",
  "X.dist_supermarket_avg",
  "X.n_trips_avg",
  "X.age_over60",
  "X.easttrue",
  "X.foreigners",
  "X.income",
  "(Intercept)",
  "QUALITYAdjRSquared",
  "QUALITYFStatistic",
  "QUALITYn"
)
variableNameNice <- c(
  "Average distance to nearest hospital in km",
  "Average distance to nearest supermarket in km",
  "Average reachable daily public transport trips",
  "Population over 60 years of age in percent",
  "Location in East German state",
  "Foreign population in percent",
  "Average yearly income in euros",
  "\\textit{Constant}",
  "Adjusted $R^2$",
  "F Statistic",
  "Number of districts $N$"
)
nicenames <- data.frame(variableName, variableNameNice)
mergemodels <- function(a, b, c, d, e, f) {
  mss_a = merge(a$model_table, b$model_table,
                  suffixes=c(a$modelname, b$modelname),
                  by="variableName", all=TRUE)
  mss_b = merge(c$model_table, d$model_table,
                  suffixes=c(c$modelname, d$modelname),
                  by="variableName", all=TRUE)
  mss_c = merge(e$model_table, f$model_table,
                  suffixes=c(e$modelname, f$modelname),
                  by="variableName", all=TRUE)
  mss = merge(mss_a, mss_b, by="variableName", all=TRUE )
  mss = merge(mss, mss_c, by="variableName", all=TRUE )
  mss = merge(mss, nicenames, by="variableName", all=TRUE)
  mss = mss[order(as.numeric(!startsWith(mss$variableName,"X.n_trips"))), ]
  mss = mss[order(as.numeric(!startsWith(mss$variableName,"X.dist_"))), ]
  mss = mss[order(as.numeric(mss$variableName == "(Intercept)")), ]
  mss = mss[order(as.numeric(startsWith(mss$variableName,"QUALITY"))), ]
  return(mss)
}


# Helper: Calculate R squared between col and polarization
r_sq <- function(col, dataset) {
  val = (cor(dataset$X.polarization, dataset[c(col)]) ^ 2)[1]
  print(paste("R-squared Polarization <->", col, ":", val))
  return(val)
}
r_sq_cols <- c("X.log_pop_density",
               "X.pop_density",
               "east_int",
               "X.n_trips_avg",
               "X.dist_supermarket_avg",
               "X.dist_hospital_avg")

# Helper: clean up names for export of cor matrix as csv
nice_var_name <- function(varname) {
  varname = gsub("X\\.dist_(\\w+)_avg", "Avg. dist. \\1", varname)
  varname = gsub("X\\.n_trips_avg", "Publ. Transp. Trips", varname)
  varname = gsub("X\\.log_pop_density", "log(Pop. density)", varname)
  varname = gsub("X\\.polarization", "Polarization", varname)
  varname = gsub("X\\.pop_density", "Population density", varname)
  return(varname)
}

# Helper for matrix of scatterplots
reg <- function (x,y,...) {
  points(x,y,...)
  abline(lm(y~x))
}

# Helper list of cities
cities <- c(
  "Mannheim",
  "Duisburg",
  "Bielefeld",
  "Darmstadt",
  "Erlangen",
  "Freiburg im Breisgau"
)

# Helper: make cor matrix, cor matrix graphic and scatter plot matrix
make_cor_matrix <- function (dname, d) {
  corobj = cor(d[corcols])
  corexport = cbind(nice_var_name(rownames(corobj)),round(corobj * 100))
  corexport = rbind(nice_var_name(colnames(corexport)),corexport)
  cor_fn = paste(dname, "_cor_matrix.csv", sep="")
  print(paste("Cor matrix:", cor_fn))
  write.csv(corexport, cor_fn, quote=FALSE, row.names=FALSE)
  corrplot(corobj, method="number", type="lower",
           main=paste("Cor. matrix for", dname))
  pairs(d[corcols], panel=reg, col=adjustcolor(4,.4),
        main=paste("Scatterplot matrix for", dname))
}

# Run evaluation for one election result
makeeval <- function (dname, d) {

  print(paste("************", dname, "************"))

  # Filter east/west sub-datasets
  deast = d[d$X.east == "true",]
  dwest = d[d$X.east == "false",]
  dnameeast = paste(dname, "east", sep="")
  dnamewest = paste(dname, "west", sep="")

  d$east_bool <- as.logical(d$X.east)
  d$east_int <- as.integer(d$east_bool)

  # Init graphics output device
  pdf_fn = paste(dname, ".pdf", sep="")
  print(paste("Graphics:", pdf_fn))
  dev.new(width=15,height=15,file=pdf_fn)

  #---------------------------------------------------------
  # Produce cor matrix and output as plot and csv
  make_cor_matrix(dname, d)
  make_cor_matrix(dnameeast, deast)
  make_cor_matrix(dnamewest, dwest)

  #---------------------------------------------------------
  # Linear models
  m_everything = makelinearmodel("m_everything", c(
              distcols,
              "X.east",
              "X.age_over60",
              "X.foreigners",
              "X.income",
              "X.social_security",
              "X.pop_density"), d)

  m_condensed = makelinearmodel("m_condensed", c(
              "X.n_trips_avg",
              "X.dist_supermarket_avg",
              "X.dist_hospital_avg",
              "X.east",
              "X.age_over60",
              "X.foreigners",
              "X.income"), d)

  cond_east = makelinearmodel("m_condensed_east", c(
                "X.n_trips_avg",
                "X.dist_supermarket_avg",
                "X.dist_hospital_avg",
                "X.age_over60",
                "X.foreigners",
                "X.income"), deast)


  cond_west = makelinearmodel("m_condensed_west", c(
                "X.n_trips_avg",
                "X.dist_supermarket_avg",
                "X.dist_hospital_avg",
                "X.age_over60",
                "X.foreigners",
                "X.income"), dwest)
    
  #---------------------------------------------------------
  # Different R squared Polarization - col
  print("R-squared for various interesting variables")
  for (col in r_sq_cols) {
    r_sq(col, d)
  }

  #---------------------------------------------------------
  # Plots for public transport
  plot(dwest$X.log_pop_density, dwest$X.n_trips_avg)
  abline(lm(dwest$X.n_trips_avg ~ dwest$X.log_pop_density))
  text(dwest$X.log_pop_density, dwest$X.n_trips_avg, labels=dwest$X.name)

  d$npolar <- (
    (d$X.polarization - min(d$X.polarization)) /
    (max(d$X.polarization) - min(d$X.polarization)))

  #---------------------------------------------------------
  # Similar pop. density, different avg. no of public transport trips and
  # different polarization.
  print("Population density - Public Transport - Polarization")
  for (city in cities) {
    tmp = d[d$X.name == city,]
    print(paste(city, "Polarization:", tmp$X.polarization,
                "Normalized Polarization:", tmp$npolar,
                "Avg. Public Transport Trips:", tmp$X.n_trips_avg,
                "Population density:", tmp$X.pop_density))
  }

  #---------------------------------------------------------
  # Subgraph with only larger cities
  dd = d[d$X.log_pop_density > 6.5,]
  plot(dd$X.log_pop_density, dd$X.n_trips_avg)
  abline(lm(dd$X.n_trips_avg ~ dd$X.log_pop_density))
  text(dd$X.log_pop_density, dd$X.n_trips_avg, labels=dd$X.name)

  dev.off()
  
  return(list("main" = m_condensed, "east" = cond_east, "west" = cond_west))
}


# Add model names and export models.csv
b <- makeeval("btw21", btw21)
btw_m <- b$main
btw_m$modelname <- "btw"
btw_e <- b$east
btw_e$modelname <- "btwe"
btw_w <- b$west
btw_w$modelname <- "btww"
e <- makeeval("ew24", ew24)
ew_m <- e$main
ew_m$modelname <- "ew"
ew_e <- e$east
ew_e$modelname <- "ewe"
ew_w <- e$west
ew_w$modelname <- "eww"
write.csv(mergemodels(btw_m, btw_e, btw_w, ew_m, ew_e, ew_w),
          "models.csv",quote=FALSE, row.names=FALSE, na="")
print("Models: models.csv")
