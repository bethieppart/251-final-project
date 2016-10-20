There are many pieces to this project, and the descriptions of these are as follows:

/final_project/dbase:
Run database_init.py to setup the database and tables for the project. Note this will be destructive to any existing data in the travel_info database if it exists. The dump_* scripts are there for debug purposes.

/final_project/data:
Run the baseline.py script to download the WGI and FSI indicators, parse the data into np.array structures, adjust for any mentions from the monthly or biannual crisis group lists and store the results in the Baseline_Data table in postgres.

/final_project/gubment:
Run the load_govt_data.sh bash script to download the Travel Alerts and Warnings issued by the US State Department. The script will call load_govt_data.py if the downloads are successful which will parse and clean the data and load it into the Govt_Data table in postgres.

/final_project/: (Twitter Monitor)
In the final_project directory, type 'sparse run' to lauch the Twitter monitoring application. This will look for key words and phrases in tweets that contain geographical data and load it into the Twitter_Data table in postgres.

/final_project/maps:
Run gen_maps.py to extract all the data sources from postgres and use to to generate two webpages based off Google's GeoChart API. One map represents the baseline data (regionChart.html where each country is colored from red to green based on it's indicator score) and the other represents the US State Department and Twitter data (markerChart.html where a marker is placed to indicate a travel alert or warning and its size represents the amount of Twitter chatter associated with that country).
