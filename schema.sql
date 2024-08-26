
CREATE TABLE properties (
    id SERIAL PRIMARY KEY,
    locationName VARCHAR(255),
    propertyName VARCHAR(255),
    propertyCode VARCHAR(255)
    );

CREATE TABLE energy_usage (
    id SERIAL PRIMARY KEY,
    locationName VARCHAR(255),
    timestamp DATE,
    energyUsage FLOAT
    );

