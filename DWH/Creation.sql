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

CREATE TABLE HighVolumeBases (
  HighVolumeLicenseNumber VARCHAR,
  LicenseNumber VARCHAR,
  BaseName VARCHAR,
  AppCompanyAffiliation VARCHAR
);

INSERT INTO HighVolumeBases (HighVolumeLicenseNumber, LicenseNumber, BaseName, AppCompanyAffiliation)
VALUES
  ('HV0002', 'B02914', 'VULCAN CARS LLC', 'Juno'),
  ('HV0002', 'B02907', 'SABO ONE LLC', 'Juno'),
  ('HV0002', 'B02908', 'SABO TWO LLC', 'Juno'),
  ('HV0002', 'B03035', 'OMAHA LLC', 'Juno'),
  ('HV0005', 'B02510', 'TRI-CITY,LLC', 'Lyft'),
  ('HV0005', 'B02844', 'ENDOR CAR & DRIVER,LLC.', 'Lyft'),
  ('HV0003', 'B02877', 'ZWOLF-NY, LLC', 'Uber'),
  ('HV0003', 'B02866', 'ZWEI-NY,LLC', 'Uber'),
  ('HV0003', 'B02882', 'ZWANZIG-NY,LLC', 'Uber'),
  ('HV0003', 'B02869', 'ZEHN-NY,LLC.', 'Uber'),
  ('HV0003', 'B02617', 'WEITER LLC', 'Uber'),
  ('HV0003', 'B02876', 'VIERZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02865', 'VIER-NY,LLC', 'Uber'),
  ('HV0003', 'B02512', 'UNTER LLC', 'Uber'),
  ('HV0003', 'B02888', 'SIEBZEHN-NY,LLC', 'Uber'),
  ('HV0003', 'B02864', 'SIEBEN-NY,LLC', 'Uber'),
  ('HV0003', 'B02883', 'SECHZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02875', 'SECHS-NY, LLC', 'Uber'),
  ('HV0003', 'B02682', 'SCHMECKEN LLC', 'Uber'),
  ('HV0003', 'B02880', 'NEUNZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02870', 'NEUN-NY,LLC', 'Uber'),
  ('HV0003', 'B02404', 'KUCHEN,LLC', 'Uber'),
  ('HV0003', 'B02598', 'HINTER LLC', 'Uber'),
  ('HV0003', 'B02765', 'GRUN LLC', 'Uber'),
  ('HV0003', 'B02879', 'FUNFZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02867', 'FUNF-NY, LLC', 'Uber'),
  ('HV0003', 'B02878', 'ELF-NY,LLC', 'Uber'),
  ('HV0003', 'B02887', 'EINUNDZWANZIG-NY, LLC', 'Uber'),
  ('HV0003', 'B02872', 'EINS-NY,LLC', 'Uber'),
  ('HV0003', 'B02836', 'DRINNEN-NY LLC', 'Uber'),
  ('HV0003', 'B02884', 'DREIZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02835', 'DREIST NY LLC', 'Uber'),
  ('HV0003', 'B02764', 'DANACH-NY,LLC', 'Uber'),
  ('HV0003', 'B02889', 'ACHTZEHN-NY, LLC', 'Uber'),
  ('HV0003', 'B02871', 'ACHT-NY,LLC', 'Uber'),
  ('HV0003', 'B02395', 'ABATAR LLC', 'Uber'),
  ('HV0004', 'B03136', 'GREENPOINT TRANSIT LLC', 'Via'),
  ('HV0004', 'B02800', 'FLATIRON TRANSIT LLC', 'Via');
