# BoardgameGeek Data Pipeline
## Problem and Objective
- 
- 
## Architecture
The pipeline consists of various modules and technologies
- **Microsoft Azure**: Azure function (orchestrating and running function), Azure Data Lake Storage (ADLS)
- **ADLS Module**: a class contains common function to help you interact with data in ADLS
- **PowerBI**: BI tool to help you connect and get data from Azure SQL Server DB for creating a interactive dashboard
### Overview
- Data is captured from the BoardgameGeek API using BGG XML API. The API documentation is available here: https://boardgamegeek.com/wiki/page/BGG_XML_API&redirectedfrom=XML_API#
- The collected data from the BoardgameGeek API is directly stored to the **Bronze** container on Azure Data Lake Storage (ADLS). ETL jobs and orchestration are written using Azure Function, **the whole data pipeline starts running when you trigger the HTTP of your Azure function**
### ETL Flow
- You trigger HTTP of **scrape_top_games** in Azure function to start a pipeline. The function will scrapes all top games of BoardgameGeek page, its parameters include: total_page and batch_size. **total_page** determines how many pages you want to scrape, each page has 100 games. **batch_size** determines how many games you want to scrape in 1 batch (highly recommend 20 or 50 for this parameter)
- After **scrape_top_games** finished, it will trigger **scrape_games_info** function to get detail information for each game. Then all batch data will be consolidated and saved to **Bronze** container
- **silver_transformation** gets that data to clean and add more useful features. Then the function saves data to **Silver** container.
- Finally, **gold_transformation** transforms data to fit business's requirements. In this case, the function creates master and bridge table and uploads it to Azure SQL Server DB. The CSV files containing master and bridge data will be saved to **Gold** container.
### PowerBI Dashboard
- 
## How to run
### Prerequisites
Install VS Code and its below extensions. 
- Azure Account
- Azure Function
- Azure Resources

You also have to install **Postman** to locally test and debug our Azure function 
### Setting up Azure resources
- 1 Azure Data Lake Storage account Gen 2 (ADLS) and 3 containers for our data layer: bronze, silver ,gold
![A D L S](png/ADLS.png)
- 1 SQL Server and 1 Database to store the data from
BoardgameGeek API
![Azure DB](png/Azure_DB.png)
### Change local config settings in local.settings.json to fit your Azure resources
- ADLS connection string and key
- Data layer containers'name in your ADLS
- Username and password of Azure SQL Server and Database
![config settings](png/config_settings.png)

### Small warning
Every Azure function folder will have the structure like the image below
- `__init__.py` contains main code to execute
- `function.json` contains a function's settings include **binding**/**trigger**

As you can see, **scrape_games_info** will start when it detects the file **bronze/top_games_info.json** being uploaded to this ADLS path. So if you want to change the scrapping file's name in our main function, you also need to change this path accordingly
![trigger binding azure function](png/trigger_binding_azure_function.png)

### Test and debug our data pipeline
Follow the path `src/py/` inside repo to open `boardgamegeek_fa` folder using VS code, then press F5 and run Postman to test the pipeline
![postman test function](png/postman_test_function.png)
=> After you've done with debug and testing, you can deploy the whole Azure Function project into cloud and run from there
