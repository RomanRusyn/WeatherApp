"""Script for parsing weather site

Script takes information from "https://openweathermap.org/".
API key for Roman is "53e054d0ccc375a2b5d0b943fcb84ee5". Need key for using
sites API.
You need id of the city for accurate results, so use function get_city_id

The Example of usage:
    python reqowm.py

    Script prints weather in cities which are located in "sample_of_cities"
    tuple. Output is like:
    Weather in {city} for today is:
    conditions: light rain
    temp: 20
    temp_min: 20
    temp_max: 20

"""
import socket
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import requests
from confluent_kafka import Producer

APPID = "53e054d0ccc375a2b5d0b943fcb84ee5"
SAMPLE_OF_CITIES = (
    "Rivne", "Kiev", "Miami", "Reykholt", "Los Angeles", "Palapye",
    "Cloncurry", "Tokyo", "Norilsk")
TOPIC = "weatherForToday"


def get_city_id(city_name):
    """Fucntion for returning id of the city"""
    try:
        result = requests.get("http://api.openweathermap.org/data/2.5/find",
                              params={'q': city_name, 'type': 'like',
                                      'units': 'metric', 'lang': 'en',
                                      'APPID': APPID})
        data = result.json()
        city_id = data['list'][0]['id']
    except Exception as e:
        print("Exception (find city):", e, " please write correct city")
        pass
    assert isinstance(city_id, int)
    return city_id


def request_current_weather(city_name):
    """Function for returning weather data"""
    city_id = get_city_id(city_name)
    try:
        result = requests.get("http://api.openweathermap.org/data/2.5/weather",
                              params={'id': city_id, 'units': 'metric',
                                      'lang': 'en', 'APPID': APPID})
        data = result.json()
        # print("conditions:", data['weather'][0]['description'])
        # print("temp:", data['main']['temp'])
        # print("temp_min:", data['main']['temp_min'])
        # print("temp_max:", data['main']['temp_max'])
        result_dict = {"conditions": data['weather'][0]['description'],
                       "temp": data['main']['temp'],
                       "humidity": data['main']['humidity'],
                       "pressure": data['main']['pressure']
                       }
        return result_dict
    except Exception as e:
        print("Exception (weather):", e)
        pass


def printing_results(results):
    for index, result in enumerate(results):
        print(f"Weather in {SAMPLE_OF_CITIES[index]} for today is:")
        print(f"conditions: {result['conditions']}")
        print(f"temp: {result['temp']}")
        print(f"humidity: {result['humidity']}")
        print(f"pressure: {result['pressure']}")


def main():
    conf = {'bootstrap.servers': "127.0.0.1:9092,192.168.99.101:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(request_current_weather,
                                    SAMPLE_OF_CITIES, timeout=12, chunksize=4))

    for res_dict in results:
        for k, v in res_dict.items():
            producer.produce(TOPIC, key=k, value=str(v), callback=acked)

    # Wait up to 1 second for events.
    producer.poll(1)

    # printing_results(results)


if __name__ == '__main__':
    while True:
        main()
        sleep(10)

    # for city in sample_of_cities:
    #     print(f"Weather in {city} for today is:")
    #     request_current_weather(get_city_id(city))
