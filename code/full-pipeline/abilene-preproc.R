###################################################################################################
### Preprocess the traffic matrix data that is used for modulating the request arrival process. ###
###################################################################################################

source("common.R")

# From http://www.cs.utexas.edu/~yzhang/research/AbileneTM.
# 1 file = 1 week in 5min intervals.
# 1 line = 1 TM with 5 different types of data: x_1,1 x_1,2 .. x_1,5 .. x_144,5.
abilene.prefix <- "../../abilene/X"
abilene.nfiles <- 24
abilene.df <- data.frame()

# Here, we use the first type of TM data, i.e., x_*,1 in the above notation.
# Summing over all entries in a given TM, we get the total traffic volume for the corresponding 5-minute interval.
for (i in 1:abilene.nfiles) {
    abilene.curweek <- fread(paste(abilene.prefix, sprintf("%02d", i), sep = ""), select = seq(1, 720, 5)) %>%
        rowSums()
    abilene.df <- rbind(abilene.df,
                        data.frame(minute = seq(0, 5 * (length(abilene.curweek) - 1), 5),
                                   totalvolume = abilene.curweek,
                                   week = i))
}

# Overview plot.
ggplot(abilene.df, aes(x = minute, y = totalvolume)) +
    geom_point() +
    facet_wrap(week ~ ., scales = "free")

# Using only week 1, normalize rate in order to scale it according to our needs later on.
# Apply moving average in order to smooth the curve.
abilene.week.1.normalized <-
    abilene.df %>%
    filter(week == 1) %>%
    mutate(normvolume = totalvolume / max(totalvolume)) %>%
    mutate(smoothvolume = roll_mean(normvolume, 30, align = "center", fill = c(first(normvolume), 0, last(normvolume)))) %>%
    mutate(smoothvolume = smoothvolume / max(smoothvolume))

ggplot(abilene.week.1.normalized, aes(x = minute / 60, y = smoothvolume)) +
    geom_point() +
    xlab("Hour") +
    ylab("Normalized Traffic Volume") +
    theme_bw(base_size = 22)

# Save data to csv for later use in SFCR trace generation.
write.table(
    abilene.week.1.normalized %>%
        select(minute, smoothvolume),
    file = "abilene-week1.csv",
    sep = ",",
    col.names = F,
    row.names = F)
