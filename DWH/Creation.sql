CREATE TABLE Fact (
  trip_id VARCHAR,
  type VARCHAR,
  VendorID INT,
  lpep_pickup_datetime timestamp,
  lpep_dropoff_datetime timestamp,
  DOLocationID INT,
  PULocationID INT,
  trip_type INT,
  store_and_fwd_flag VARCHAR,
  trip_distance FLOAT,
  passenger_count INT,
  payment_type VARCHAR,
  RateCode VARCHAR,
  extra FLOAT,
  congestion_surcharge FLOAT,
  mta_tax FLOAT,
  fare_amount FLOAT,
  ehail_fee VARCHAR,
  tolls_amount FLOAT,
  tip_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT
);

COPY INTO fact
FROM @my_stage/consumer_output.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  SKIP_HEADER = 1
);



CREATE TABLE Location (
  LocationID INT,
  Borough VARCHAR,
  Zone VARCHAR,
  service_zone VARCHAR
);

COPY INTO Location (LocationID, Borough, Zone, service_zone)
FROM @my_stage/locs.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);
