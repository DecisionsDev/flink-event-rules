-- =============================================================================
-- Flink SQL Enrichment: Airline Flight Events
-- Enriches FLIGHTOPEN events with:
--   - Flight specs    (aircraftId, type, route, duration)        from flights.csv
--   - Passengers list (join table + global registry)             from flight_passengers.csv + passengers.csv
--   - Catering info   (meal counts per cabin class)              from catering.csv
-- Non-FLIGHTOPEN events (e.g. FLIGHTDELAYED, FLIGHTINFO) pass through untouched.
-- =============================================================================

-- 1a. Reference Table: Flight specs
CREATE TABLE csv_flights (
    flightNumber STRING,
    aircraftId   STRING,
    aircraftType STRING,
    `date`       STRING,
    origin       STRING,
    destination  STRING,
    duration     STRING,
    PRIMARY KEY (flightNumber) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path'      = 'samples/airline-agent/enrichment/flights.csv',
    'format'    = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- 1b. Reference Table: Global passenger registry — one row per passenger.
--     Identity data that does not change per flight.
CREATE TABLE csv_passengers (
    passengerId      STRING,
    `name`           STRING,
    birthdate        STRING,
    citizenship      STRING,
    residenceCountry STRING,
    loyaltyStatus    STRING,
    PRIMARY KEY (passengerId) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path'      = 'samples/airline-agent/enrichment/passengers.csv',
    'format'    = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- 1c. Reference Table: Flight-passenger join table — one row per (flight, passenger) pair.
--     Carries only flight-specific passenger attributes.
CREATE TABLE csv_flight_passengers (
    flightNumber  STRING,
    passengerId   STRING,
    checkInStatus STRING,
    travelClass   STRING
) WITH (
    'connector' = 'filesystem',
    'path'      = 'samples/airline-agent/enrichment/flight_passengers.csv',
    'format'    = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- 1d. Reference Table: Catering information — one row per flight.
CREATE TABLE csv_catering (
    flightNumber           STRING,
    economyTotalMealCount  INT,
    businessTotalMealCount INT,
    firstTotalMealCount    INT,
    PRIMARY KEY (flightNumber) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path'      = 'samples/airline-agent/enrichment/catering.csv',
    'format'    = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- 2. Pre-aggregation View: join flight_passengers with the passenger registry,
--    then JSON_ARRAYAGG(JSON_OBJECT(...)) builds a JSON array string per flight.
--    NOTE: Flink 2.2 JSON_OBJECT treats this STRING as a literal when used as VALUE,
--    so the Java layer (SqlEnrichmentRunner) re-parses any escaped JSON strings
--    back into JsonNode after readTree(), transparently fixing the escaping.
CREATE TEMPORARY VIEW passengers_per_flight AS
SELECT
    fp.flightNumber,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'passengerId'      VALUE fp.passengerId,
            'name'             VALUE p.`name`,
            'birthdate'        VALUE p.birthdate,
            'citizenship'      VALUE p.citizenship,
            'residenceCountry' VALUE p.residenceCountry,
            'loyaltyStatus'    VALUE p.loyaltyStatus,
            'check-inStatus'   VALUE fp.checkInStatus,
            'travelClass'      VALUE fp.travelClass
        )
    ) AS passengers_json
FROM csv_flight_passengers AS fp
LEFT JOIN csv_passengers AS p ON fp.passengerId = p.passengerId
GROUP BY fp.flightNumber;

-- 3. Enriched View:
-- For FLIGHTOPEN events, builds the enriched payload. The passengers_json column
-- will be an escaped JSON string in the output — the Java layer unescapes it.
-- All other event types pass through with raw_payload unchanged.
CREATE TEMPORARY VIEW enriched_flight_events AS
SELECT
    r.`timestamp`  AS `timestamp`,
    r.flightNumber AS entity_id,
    CASE
        WHEN r.`type` = 'FLIGHTOPEN' THEN
            JSON_OBJECT(
                'timestamp'    VALUE r.`timestamp`,
                'flightNumber' VALUE r.flightNumber,
                'type'         VALUE r.`type`,
                'flight'       VALUE JSON_OBJECT(
                    'flightNumber'        VALUE r.flightNumber,
                    'aircraftId'          VALUE spec.aircraftId,
                    'aircraftType'        VALUE spec.aircraftType,
                    'date'                VALUE spec.`date`,
                    'origin'              VALUE spec.origin,
                    'destination'         VALUE spec.destination,
                    'duration'            VALUE spec.duration,
                    'cateringInformation' VALUE JSON_OBJECT(
                        'economyTotalMealCount'  VALUE cat.economyTotalMealCount,
                        'businessTotalMealCount' VALUE cat.businessTotalMealCount,
                        'firstTotalMealCount'    VALUE cat.firstTotalMealCount
                    ),
                    'passengers'          VALUE pax.passengers_json
                )
            )
        ELSE
            r.raw_payload
    END AS payload_json
FROM raw_flight_events AS r
LEFT JOIN csv_flights           AS spec ON r.`type` = 'FLIGHTOPEN' AND r.flightNumber = spec.flightNumber
LEFT JOIN csv_catering          AS cat  ON r.`type` = 'FLIGHTOPEN' AND r.flightNumber = cat.flightNumber
LEFT JOIN passengers_per_flight AS pax  ON r.`type` = 'FLIGHTOPEN' AND r.flightNumber = pax.flightNumber;
