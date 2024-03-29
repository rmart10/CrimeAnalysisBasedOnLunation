# CrimeAnalysisBasedOnLunation

After recently completing DataCamp's Python Programmer Track, I decided to put those skills to work. I was able to get ahold of a large dataset representing NYC's crime statistics for the last decade +. My goal was to determine if there was a relationship between lunar phases/lunation and crime volumes.

The python scripts in this repo will:

1. Download a copy of the source files from my google docs repo to:
"C:/tempz/Moon Phase Crime Analysis" -- feel free to change on ~line 67
2. Read the complaint number, complaint dates from the data set into a pandas dataframe, by chunk, only keeping records from year 2013 and up (volume of crimes didnt seem reliable in years prior)
3. Output that dataset.
4. Using the minimized data set, load into a new pandas dataframe and aggregate crime volumes by date, stage those results into a staging data set.
5. The staged data set (complaint volumes by date) is then used to obtain the lunation phase for each date in the date set, using either python ephem.Moon(), Observer() or a function I re used from this post: https://www.daniweb.com/programming/software-development/code/453788/moon-phase-at-a-given-date-python, depending on which file you use.
6. This final data set (complaint volume, date, lunar phase) is then used to calculate the correlation coefficient (Pearson's) to determine the relationship between crime volumes on a given day and the lunar phase.

While the correlation is positive, at least according to the data, it is weak.

Based on the different lunar phases returned in either; the py ephem or daniweb approach,for given dates in the sample:
>The ephem based calc correlation coefficient returned is: 0.04125095348713485

>The daniweb based function calc correlation coefficient returned is: 0.03474831585490815

If not running on windows, change output files path on ~line 67: file_path = "C:/tempz/Moon Phase Crime Analysis"



