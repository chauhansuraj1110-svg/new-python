# file: weather_api.py
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Sample data
weather_data = {
    "New York": {"temp": 5, "condition": "Cloudy"},
    "London": {"temp": 10, "condition": "Rainy"},
    "Mumbai": {"temp": 28, "condition": "Sunny"}
}

class City(BaseModel):
    name: str

@app.post("/get_weather")
def get_weather(city: City):
    city_name = city.name
    info = weather_data.get(city_name)
    if info:
        return {"city": city_name, "weather": info}
    else:
        return {"error": "City not found"}
