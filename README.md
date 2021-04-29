# NYC MTA BusTime REPROCESSOR
- v1 2021 Apr 28
- Anthony Townsend <atownsend@cornell.edu>


### why i'm dumb

the way that nycbuswatcher creates the daily archives is nasty. all the SIRI responses are concatenated into a single file, but without line breaks so its impossible to read and even `awk` runs out of memory.

# spliting the archives

`awk '/{"Siri"/{x="F"++i;}{print > x;}' daily-2021-01-24`





# TO DO AS OF APRIL 28 IN NOTEBOOK
1. dates as kwargs
    - start = 2021-04-01
    - end = 2021-04-15
2. verify the daily .gz files exist
3. loop over each day's file
    a. extract each individual route files to a NEW (for debugging) temp/date/rt_name folder
    b. create a list of tuples (response_filename,timestamp) from directory listing
    c. loop over response_file list for that date/route combo
        i. open the response
        ii. parse all the observations into a df (using the full, new parser from Dumpers.py?)
        iii. add it to a df (try one for whole date_citywide, if too big do each route separate)
        iv. dump the df to a CSV
        v. NEVER WRITE ANYTHING TO A DATABASE
    z. empty temp folder


### importing / updating production db

This is probably a bad idea. Need to migrate the main db when I deploy the next version of `master` branch, but for now, can leave this to the side. or if want to add this to the existing db, can manually import / merge with existing records.
