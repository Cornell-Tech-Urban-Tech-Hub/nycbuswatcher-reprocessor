# NYC MTA BusTime REPROCESSOR
- v1 2021 Apr 30
- Anthony Townsend <atownsend@cornell.edu>


### usage

##### `python archive-reprocessor.py`

The script will look in `./data` for any files in the form of `daily-YYYY-MM-DD.gz` and starting form the earliest date does the following:

1. Unzips the archive to the current folder with the name structure `daily-YYYY-MM-DD.json.gz`
2. Begins loading the JSON as a stream, pulling out each `Siri` response, which represents a single route for a single point in time.
3. Parses each `MonitoredVehicleJourney` into a `BusObservation` class instance, and adds that to a database session. The session is committed after each `Siri` response is parsed.
4. Writes each day's data to a single `daily-YYYY-MM-DD.sqlite3` file.

Each daily file takes about 30 minutes to parse on a typical desktop computer.

##### `python sqlite3_to_df.py` (untested)

Crawls the same tree for the resulting `sqlite3` files and loads each into its own dataframe, returning a list, until you run out of patience or memory?
### why this was so complicated

1. The files are big—like 10Gb.
2. The way that `nycbuswatcher` creates the daily archives is nasty. All the `Siri` responses are concatenated into a single file, but as of 2021-04-30 I hadn't added line breaks! So its impossible to read in python without running out of memory, and even `awk` runs out of memory. So it ook a while to find `ijson` and use a lazy loading generator approach.

##### speeding up with pypy

1. on mac, brew install pypy
2. create a conda env with pypy 
    ```conda config --set channel_priority strict
     conda create -c conda-forge -n pypy pypy
     conda activate pypy
     conda install sqlalchemy
     ```
3. `pypy -mensurepip --default-pip` not sure what this does [but](https://doc.pypy.org/en/latest/faq.html#module-xyz-does-not-work-with-pypy-importerror)...
4. `pip install ijson`
3. run as normal, should be about 

### process to pull the archives from server

make a filename list 

`cat response_filelist_all.txt | grep 2021-0[1-2]-* > filelist.txt`

check it and then construct something to batch copy from container to host

`docker cp nycbuswatcher_grabber_1:/app/data/daily-2021-01-24.gz .`

---> #todo dockerize it, add it to the stack, and reprocess the files on the same bus_data volume ina new folder?

