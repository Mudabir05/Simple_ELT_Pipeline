-- combine_data.sql
CREATE TABLE combined_data AS
SELECT     
    p.locationName,
    p.propertyName,
    p.propertyCode,
    e.timestamp,
    e.energyUsage
FROM 
    properties p
JOIN 
    energy_usage e
ON 
    p.locationName = e.locationName;