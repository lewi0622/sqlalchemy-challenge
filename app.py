from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# setup sqlalchemy
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
base = automap_base()
# reflect the tables
base.prepare(engine, reflect=True)

# tables
measurement = base.classes.measurement
station = base.classes.station


def get_stations():
    """Returns list of stations"""
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Get all stations
    all_stations_df = pd.DataFrame(session.query(measurement.station))

    # Close session
    session.close()
    return all_stations_df


def one_year_date():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Calculate the date 1 year ago from the last data point in the database
    last_date = [*session.query(func.max(measurement.date))][0][0]
    date = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Close session
    session.close()

    return date


def start_end_date(start_date, end_date=None):
    # Convert the start date to datetime
    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    if end_date:
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Create our session (link) from Python to the DB
    session = Session(engine)
    if end_date:
        date_tobs_df = pd.DataFrame(session.query(measurement.tobs).filter(measurement.date > start_date).filter(measurement.date < end_date))
    else:
        date_tobs_df = pd.DataFrame(session.query(measurement.tobs).filter(measurement.date > start_date))

    # Close session
    session.close()

    # check if query is empty
    if date_tobs_df.empty:
        return "Given date returned no records. Please try different date."

    return jsonify({'Min': date_tobs_df.min()[0], 'Max': date_tobs_df.max()[0], 'Avg': round(date_tobs_df.mean()[0], 2)})


# Flask setup
app = Flask(__name__)

# Routes
@app.route("/")
def welcome():
    return (
        f"""Welcome to the home page.<br>
        Available Routes are:<br>
        /api/v1.0/precipitation<br>
        /api/v1.0/stations<br>
        /api/v1.0/tobs<br>
        /api/v1.0/start-date<br>
        /api/v1.0/start-date/end-date
        <br><br>
        For start-date and end-date, please use yyyy-mm-dd format.
        """
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Returns the date and precipitation data from the last year"""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Perform a query to retrieve the data and precipitation scores
    twelve_month_precip = session.query(measurement.date, measurement.prcp).filter(measurement.date > one_year_date())

    # Close session
    session.close()

    # return the results as a json
    return jsonify(pd.DataFrame(twelve_month_precip).to_dict(orient='records'))


@app.route("/api/v1.0/stations")
def stations():
    """Returns the list of stations"""
    stations_df = get_stations()

    # Drop duplicates
    stations_df.drop_duplicates(inplace=True)
    
    # Return list of stations
    return jsonify(stations_df.to_dict(orient='records'))


@app.route("/api/v1.0/tobs")
def tobs():
    """Returns the temperature data over the last year from the most active station"""
    stations_df = get_stations()

    # find the most active station
    station_activity_df = pd.DataFrame(stations_df['station'].value_counts())
    most_active_station = station_activity_df.reset_index()['index'][0]

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # query the most active station over the last year
    twleve_month_tobs = session.query(measurement.date, measurement.tobs).filter(measurement.date > (one_year_date())).filter(measurement.station == most_active_station)

    # Close session
    session.close()

    return jsonify(pd.DataFrame(twleve_month_tobs).to_dict(orient='records'))


@app.route("/api/v1.0/<start_date>")
def start_only(start_date):
    """Returns the min, max, and average temperatures 
    for the given start date till the end of the data"""
    return start_end_date(start_date)


@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end(start_date, end_date):
    """Returns the min, max, and average temperatures 
    for the given start date till the end date."""
    return start_end_date(start_date, end_date)


if __name__ == "__main__":
    app.run(debug=True)