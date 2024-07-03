# American Airline Companies Data Analysis
Big Data project for purposes of "Architectures of Big Data Systems" course. 
The dataset for batch processing is available at [link](https://www.kaggle.com/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018) and years used in batch processing dates from 2013 up to 2017. 

The project aims to answer the following questions using batch processing:
- Which is the worst/best airport for arrivals?
- Which is the worst/best airport for departures?
- What are the top N worst/best airlines overall?
- What is the most critical month for delays and cancellations?
- What is the most critical period of the year, and how does it correlate with holidays?
- What is the most critical day of the week for delays?
- What is the worst period of the day for delays (e.g., within a 3-hour window)?
- Which airline has the smallest difference between scheduled time and elapsed time?
- Which airline has the largest difference between scheduled time and elapsed time?
- Is there a higher likelihood of delays for shorter or longer flights?
- Which airline has the most cancellations within a specific period?
- What are the worst states for delays overall?
- How do airports rank within each state based on delay metrics?
- How has the number of flights changed over the years?
- What is the popularity of flights by month?
- What are the most popular destinations, and how do they rank by month?
- Which state has the highest number of outbound flights, and how does this vary by month?

Stream processing will pull tweets from [X](x.com) (formerly Twitter) and answer following questions:
- How do different types of adverse weather conditions rank in terms of their impact on flight delays and cancellations?
- How do the severity levels of weather conditions rank in terms of their impact on flight operations?
- How do adverse weather conditions affect different airlines, and how do they rank based on their ability to handle such conditions?
- How do different states rank based on the impact of adverse weather conditions on flights?
- What is the correlation between geographic latitude and longitude and the severity of flight delays caused by adverse weather conditions?

## Project start
When downloaded _csv_ files, they should be placed in _local-data_ folder and renamed to _batch-yyyy.csv_.

Start Docker compose from `docker-spec` folder and run [import_data.sh](import-data/import_data.sh) script.

To run batch processing execute: [run.sh](spark/batch/run.sh)
To run stream processing execute: [run.sh](spark/real-time/run.sh)

