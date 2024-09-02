import datetime
from pymongo import MongoClient
import time
from pms_a003 import Sensor

# MongoDB Atlas setup
uri = "mongodb+srv://user:pass@dbname.m0bml.mongodb.net/?retryWrites=true&w=majority&appName=dbname"
client = MongoClient(uri)
db = client['air_quality_data']
collection = db['sensor_readings']

# Initialize sensor
air_mon = Sensor()
air_mon.connect_hat(port="/dev/ttyAMA0", baudrate=9600)

# Setting up a TTL index for data expiration
collection.create_index([("timestamp", 1)], expireAfterSeconds=86400)  # 86400 seconds = 24 hours

def send_data_to_atlas(pm1, pm25, pm10):
    """
    Function to send data to MongoDB Atlas.
    """
    data = {
        "PM1.0": pm1,
        "PM2.5": pm25,
        "PM10": pm10,
        "timestamp": datetime.datetime.utcnow()
    }
    try:
        collection.insert_one(data)
        print("Data posted successfully: ", data)
    except Exception as e:
        print("An error occurred while posting data: ", e)

def read_sensor_data():
    """
    Function to read data from the air quality sensor.
    """
    values = air_mon.read()
    return values.pm10_cf1, values.pm25_cf1, values.pm100_cf1

try:
    while True:
        pm10, pm25, pm1 = read_sensor_data()
        send_data_to_atlas(pm1, pm25, pm10)
        time.sleep(5)  # Send data every 5 seconds
except KeyboardInterrupt:
    print("Stopping the sensor data collection...")
finally:
    air_mon.disconnect_hat()
    print("Disconnected the sensor.")
