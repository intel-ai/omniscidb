USER admin omnisci {

DROP TABLE IF EXISTS flights;

CREATE DATAFRAME flights(
    Year smallint,
    Month smallint,
    DayofMonth smallint,
    DayOfWeek smallint,
    DepTime smallint,
    CRSDepTime smallint,
    ArrTime smallint,
    CRSArrTime smallint,
    UniqueCarrier text encoding dict,
    FlightNum smallint,
    TailNum text encoding dict,
    ActualElapsedTime smallint,
    CRSElapsedTime smallint,
    AirTime smallint,
    ArrDelay smallint,
    DepDelay smallint,
    Origin text encoding dict,
    Dest text encoding dict,
    Distance smallint,
    TaxiIn smallint,
    TaxiOut smallint,
    Cancelled smallint,
    CancellationCode text encoding dict,
    Diverted smallint,
    CarrierDelay smallint,
    WeatherDelay smallint,
    NASDelay smallint,
    SecurityDelay smallint,
    LateAircraftDelay smallint)
    FROM 'CSV:SampleData/100_flights.csv' WITH(header='true');

    select count(*) from flights where LateAircraftDelay >= 10;
}
