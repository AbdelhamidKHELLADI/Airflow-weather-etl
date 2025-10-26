# 🌦️ Weather ETL with Apache Airflow

## 📋 Overview
A simple **ETL pipeline** using **Apache Airflow** that fetches weather data from the **OpenWeather API**, processes it, and stores it in CSV format every hour.



## ⚙️ Setup
### Clone project
```bash
git clone https://github.com/AbdelhamidKHELLADI/Airflow-weather-etl.git
cd Airflow-weather-etl
```
### Install Airflow  
Follow the [official guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html#) 

### Add API Key  
You can get an API key from [OpenWeather](https://openweathermap.org/api)  

Add your **OpenWeather API key** as an Airflow Variable:

**UI:**  
Go to *Admin → Variables → +*  
- Key: `OPEN_WEATHER_API`  
- Value: your_api_key  

**or CLI:**  
```bash
airflow variables set OPEN_WEATHER_API your_api_key
```

---

### Project Structure
```
~/airflow/
├── dags/
│   └── weather_etl.py
├── tmp/
├── transformed/
└── cities.txt
```

- `cities.txt` → contains `city,lat,lon`  
- `tmp/` → raw weather data  
- `transformed/` → processed CSV files  

---

### Run the DAG
Copy your DAG:
```bash
cp weather_etl.py ~/airflow/dags/weather_etl.py
```

Then check Airflow UI → enable `weather_etl`.

---

### How It Works
| Step | Task | Description |
|------|------|--------------|
| Extract | `extract()` | Fetches weather data via OpenWeather API |
| Transform | `transform()` | Cleans and formats data |
| Load | `load()` | Appends to CSV in `/transformed` |

Runs **hourly** by default.

---

### Cities
Copy the cities file to airflow
```bash
cp cities.txt ~/airflow
```
---
In `cities.txt`:
```
Trento,46.0667,11.1167
Paris,48.8566,2.3522
```
To avoid calling [GeoCoding API](https://openweathermap.org/api/geocoding-api) to get latitude and longitude every time for the same city, it will be stored in `cities.txt`  
You can change the `CITY` value to get another city


---
**Tags:** Airflow, ETL, OpenWeather, Automation
