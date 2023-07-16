CREATE TABLE Fact_green_yellow (
  trip_id VARCHAR ,
  type VARCHAR,
  VendorID INT,
  pickup_datetime VARCHAR,
  dropoff_datetime VARCHAR,
  DOLocationID INT,
  PULocationID INT,
  trip_type VARCHAR,
  store_and_fwd_flag VARCHAR,
  trip_distance FLOAT,
  passenger_count INT,
  payment_type VARCHAR,
  ratecode VARCHAR,
  extra FLOAT,
  congestion_surcharge FLOAT,
  mta_tax FLOAT,
  fare_amount FLOAT,
  ehail_fee VARCHAR,
  tolls_amount FLOAT,
  tip_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  airport_fee FLOAT
);

ALTER TABLE fact_green_yellow
ADD CONSTRAINT pk_fact_green_yellow PRIMARY KEY (trip_id);

COPY INTO FACT_GREEN_YELLOW
FROM @my_stage/data_folder/2023-07-16/
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);



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
