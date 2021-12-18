# Data_engineering_2_project
Project Title : husHUsh Recruiter In this project we developed a data pipeline for streaming weather data considering data ingestion , data storage and data visualization.
Performed data ingestion with the help of a API call using python programming
Used Kafka as Buffer storage  . Data was stored in NoSQL database MongoDB
Performed visualization using Jupyter notebook.
Motivation The main idea in trying to develop the project is to understand how we could use python programming languange, apache kafka and MongoDB  to access streaming data.
Build Status The project is in completed status with no bugs or issues
Code Style We have used standard code style in this project using python programming language
Tech/Framework used This project was developed using python programming with the help of pandas data frame , MongoDB sink connector to store and visualize the data.
Features Application of confluent kafka to store streaming data
Code Examples

#To call weather API source

def write_to_queue(p,lat, lon):
    while True:
            try:
                response = requests.get("https://api.openweathermap.org/data/2.5/onecall?lat="+str(lat)+"&lon="+str(lon)+"&exclude=hourly,daily&appid=0170303693f51ac954ab9e1f360d2dc9")
                print(response.json())
                if (response.json!=401) & (response!=400):
                    msg_value=response.json()
                    msg_header = {"source" : b'DEM'}
                    p.poll(timeout=0)
                    p.produce(topic='weather', value=str(msg_value), headers=msg_header, on_delivery=delivery_report)
                break
            except BufferError as buffer_error:
                    print(f"{buffer_error} :: Waiting until Queue gets some free space")
                    time.sleep(1)
    p.flush()
    
    # Setup MongoDB sink connector in Confluent CLoud Kafka
    
    #Establish connection between mongo DB and python
    
    #Establishing connection between mongodb cluster and application
client = pymongo.MongoClient("mongodb+srv://aniket5511:aniket5511@cluster0.2pqbq.mongodb.net/weather?retryWrites=true&w=majority")
db = client['weather']
records = db['weatherAPI_2']

    #Visualization of data in Jupyter Notebook
    
    # Finding correlation between different attributes by plotting heatmap 
import seaborn as sns
#Using Pearson Correlation
plt.figure(figsize=(12,10))
cor = df_3.corr()
sns.heatmap(cor, annot=True)
plt.show()
# Current temperature and dew point are highly correlated

0.How to Use? The project has been split into 3 parts which occurs sequentially

Data ingestion - with the help of a API call using python programming
Data storage - using confluent kafka as producer and mongoDB sink connector as consumer. Data is stored in MongoDB Atlas cluster
Data visualization- using Jupyter notebook
    
