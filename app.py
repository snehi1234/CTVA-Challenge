import traceback

from flask import request, jsonify
import logging, time
import os
from datetime import datetime
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Float, func, Integer

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///mydatabase.db'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
db = SQLAlchemy(app)

if os.path.exists('app.log'):
    os.remove('app.log')

# Configure logging with timestamp
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)


class WeatherData(db.Model):
    rowId = db.Column(db.String(30), nullable=False, primary_key=True)
    weatherStation = db.Column(db.String(20), nullable=False)
    dateMMYY = db.Column(db.String(8), nullable=False)
    maxTemp = db.Column(db.Integer, nullable=True)
    minTemp = db.Column(db.Integer, nullable=True)
    precipitation = db.Column(db.Integer, nullable=True)

class statistics(db.Model):
    id = Column(Integer, primary_key=True, autoincrement=True)
    weatherStation = db.Column(db.String(20), nullable=False)
    year = db.Column(db.String(8), nullable=False)
    avgMxTemp = db.Column(Float(precision=2), nullable=True)
    avgMnTemp = db.Column(Float(precision=2), nullable=True)
    precipSum = db.Column(db.Integer, nullable=True)


# Function to insert or update data into the User table
def insert_or_update_data(station_name, record_date, max_temp, min_temp, precipitation):
    try:
        new_data = WeatherData(rowId=str(station_name) + str(record_date), weatherStation=station_name,
                               dateMMYY=record_date, maxTemp=max_temp, minTemp=min_temp, precipitation=precipitation)
        db.session.add(new_data)
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        # logging.error('Error inserting/updating data at %s: %s', datetime.datetime.now(), str(e))
    finally:
        db.session.close()
    return False


@app.before_first_request
def before_first_request():
    with app.app_context():
        db.create_all()

@app.route('/', methods=['GET'])
def home():
    try:
        starttime = time.time()
        directory_path = os.getcwd()
        parent_directory = os.path.dirname(directory_path)
        directory_path = os.path.join(parent_directory, 'code-challenge-template/wx_data')

        count = 0
        for filename in os.listdir(directory_path):
            if filename.endswith('.txt'):
                file_path = os.path.join(directory_path, filename)
                station_name = filename.split('.')[0]
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                    for row in lines:
                        row = row.rstrip("\n").split("\t")

                        row[0] = row[0][:4] + '/' + row[0][4:6] + '/' + row[0][6:]
                        date = datetime.strptime(row[0], '%Y/%m/%d').strftime('%Y-%m-%d')
                        max_temp = int(row[1])
                        min_temp = int(row[2])
                        precipitation = int(row[3])

                        if insert_or_update_data(station_name, date, max_temp, min_temp, precipitation):
                            count += 1

        endtime = time.time()
        if count != 0:
            logging.error('Insertion starttime - %s', time.strftime("%H:%M:%S", time.localtime(starttime)))
            logging.error('Records Inserted: %s', count)
            logging.error('Insertion endtime - %s', time.strftime("%H:%M:%S", time.localtime(endtime)))

        ###########################################
        # # insert data into stats table
        ###########################################
        def extract_year(date):
            return func.extract('year', date)

        # SQLAlchemy query to calculate statistics
        query = db.session.query(
            WeatherData.weatherStation.label('station'),
            extract_year(WeatherData.dateMMYY).label('year'),
            func.avg(WeatherData.maxTemp).label('avg_max_temp'),
            func.avg(WeatherData.minTemp).label('avg_min_temp'),
            func.sum(WeatherData.precipitation).label('total_precip')
        ).group_by('station', 'year')

        # Execute the query and fetch results
        results = query.all()
        # Create instances of the Statistics model and add them to the database session
        for result in results:
            stats_instance = statistics(
                weatherStation=result['station'],
                year=result['year'],
                avgMxTemp=result['avg_max_temp'],
                avgMnTemp=result['avg_min_temp'],
                precipSum=result['total_precip']
            )
            db.session.add(stats_instance)
        db.session.commit()

        return {}
    except Exception as e:
        return {"status": "500", "message": str(e)}, 500

@app.route('/api/weather', methods=['GET'])
def index():
    data = request.json
    weather_station = data.get('weatherStation')
    date_mmyy = data.get('dateMMYY')

    if weather_station and date_mmyy:
        weather_data = WeatherData.query.filter_by(weatherStation=weather_station, dateMMYY=date_mmyy).all()
    elif weather_station:
        weather_data = WeatherData.query.filter_by(weatherStation=weather_station).all()
    elif date_mmyy:
        weather_data = WeatherData.query.filter_by(dateMMYY=date_mmyy).all()
    else:
        weather_data = WeatherData.query.all()

    output = []
    for data in weather_data:
        output.append({
            "rowId": data.rowId,
            "weatherStation": data.weatherStation,
            "dateMMYY": data.dateMMYY,
            "maxTemp": data.maxTemp,
            "minTemp": data.minTemp,
            "precipitation": data.precipitation
        })

    #return render_template('weather.html', search_results=output)
    return { "weather_data": output }


@app.route('/api/weather/stats', methods=['GET'])
def stats():
    try:
        data = request.json
        weather_station = data.get('weatherStation')
        year = data.get('year')
        print(weather_station, year)
        # Check if at least one parameter is provided
        if not year and not weather_station:
            return {"status": "400", "message": "Please provide at least one parameter (year or weatherStation)"}

        response_data = {}
        stats = statistics.query.all()
        statsData = []
        for row in stats:
            statsData.append({
                "weatherStation": row.weatherStation,
                "year": row.year,
                "avgMxTemp": row.avgMxTemp,
                "avgMnTemp": row.avgMnTemp,
                "precipSum": row.precipSum
            })

        # Check if both parameters are provided
        if year and weather_station:
            for entry in statsData:
                if entry["year"] == year and entry["weatherStation"] == weather_station:
                    response_data['average_avgMxTemp'] = entry["avgMxTemp"]
                    response_data['average_avgMnTemp'] = entry["avgMnTemp"]
                    response_data['total_precipSum'] = entry["precipSum"]
        else:
            if year:
                count, mx_temp_sum, mn_temp_sum, precip_sum = 0, 0, 0, 0
                for entry in statsData:
                    if entry["year"] == year:
                        mx_temp_sum += entry["avgMxTemp"]
                        mn_temp_sum += entry["avgMnTemp"]
                        precip_sum += entry["precipSum"]
                        count += 1
                print(mx_temp_sum, mn_temp_sum, precip_sum)
                if count > 0:
                    response_data["average_avgMxTemp"] = mx_temp_sum / count
                    response_data["average_avgMnTemp"] = mn_temp_sum / count
                    response_data["total_precipSum"] = precip_sum
            else:
                count, mx_temp_sum, mn_temp_sum, precip_sum = 0, 0, 0, 0
                for entry in statsData:
                    if entry["weatherStation"] == weather_station:
                        mx_temp_sum += entry["avgMxTemp"]
                        mn_temp_sum += entry["avgMnTemp"]
                        precip_sum += entry["precipSum"]
                        count += 1
                if count > 0:
                    response_data["average_avgMxTemp"] = mx_temp_sum / count
                    response_data["average_avgMnTemp"] = mn_temp_sum / count
                    response_data["total_precipSum"] = precip_sum


        return {"status": "200", "data": response_data}

    except Exception as e:
        return {"status": "500", "message": str(e)}, 500


"""
@app.route('/api/weather/stats', methods=['GET'])
def stats():
    try:
        data = request.json
        weather_station = data.get('weatherStation')
        year = data.get('year')

        # Check if at least one parameter is provided
        if year is None and weather_station is None:
            return { "status": "200", "Message": "Please input the requirements"}

        response_data = {}
        # Check if both parameters are provided
        if year is not None and weather_station is not None:
            # Filter data for the given year
            query = db.session.query(
                func.avg(statistics.avgMxTemp).label('average_avgMxTemp'),
                func.avg(statistics.avgMnTemp).label('average_avgMnTemp'),
                func.sum(statistics.precipSum).label('total_precipSum')
            ).filter(statistics.year == year, statistics.weatherStation == weather_station).group_by(statistics.year, statistics.weatherStation).all()
            #result = query.first()
            for row in query:
                print(row)
            # if result:
            #     response_data = {
            #         'average_avgMxTemp': result.average_avgMxTemp,
            #         'average_avgMnTemp': result.average_avgMnTemp,
            #         'total_precipSum': result.total_precipSum,
            #     }

        
        if year:
            query = db.session.query(
                func.avg(statistics.avgMxTemp).label('average_avgMxTemp'),
                func.avg(statistics.avgMnTemp).label('average_avgMnTemp'),
                func.sum(statistics.precipSum).label('total_precipSum')
            ).filter(statistics.year == year).group_by(statistics.year).all()
            result = query.all()
            if result:
                response_data = {
                    'average_avgMxTemp': result.average_avgMxTemp,
                    'average_avgMnTemp': result.average_avgMnTemp,
                    'total_precipSum': result.total_precipSum,
                }

        if weather_station:
            query = db.session.query(
                func.avg(statistics.avgMxTemp).label('average_avgMxTemp'),
                func.avg(statistics.avgMnTemp).label('average_avgMnTemp'),
                func.sum(statistics.precipSum).label('total_precipSum')
            ).filter(statistics.weatherStation == weather_station).group_by(statistics.weatherStation).all()
            result = query.all()
            if result:
                response_data = {
                    'average_avgMxTemp': result.average_avgMxTemp,
                    'average_avgMnTemp': result.average_avgMnTemp,
                    'total_precipSum': result.total_precipSum,
                }
        

        return {
            "status": 200,
            "data": response_data
        }
    except Exception as e:
        resp = {
            'response_code': "230",
            'response_message': traceback.print_exc(),
            'error': str(e)
        }

    return resp
    return

    # This code assumes you have already defined the WeatherData model and initialized your Flask app.
"""

if __name__ == '__main__':
    app.run()
