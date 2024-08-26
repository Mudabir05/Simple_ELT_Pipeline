CREATE TABLE energy_usage (
    id SERIAL PRIMARY KEY,
    locationName VARCHAR(255),
    timestamp DATE,
    energy_usage FLOAT
    );
