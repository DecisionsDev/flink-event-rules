-- =============================================================================
-- Flink SQL Enrichment: Outbound Passenger Notifications
-- Enriches PassengerNotification events with passenger contact details:
--   - name, loyaltyStatus, email, phone, contactPreference  from passengers.csv
-- Events whose passengerNumber has no matching registry row pass through with
-- contact fields absent (LEFT JOIN semantics).
-- =============================================================================

-- 1. Reference Table: Global passenger registry
CREATE TABLE csv_passengers_notif (
    passengerId       STRING,
    `name`            STRING,
    birthdate         STRING,
    citizenship       STRING,
    residenceCountry  STRING,
    loyaltyStatus     STRING,
    email             STRING,
    phone             STRING,
    contactPreference STRING,
    PRIMARY KEY (passengerId) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path'      = 'samples/airline-agent/enrichment/passengers.csv',
    'format'    = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

-- 2. Enriched View:
-- Joins each PassengerNotification with the passenger registry on passengerNumber,
-- injecting contact details into the payload. The output uses the same three-column
-- contract expected by SqlEnrichmentRunner: timestamp, entity_id, payload_json.
CREATE TEMPORARY VIEW enriched_passenger_notifications AS
SELECT
    n.`timestamp`    AS `timestamp`,
    n.passengerNumber AS entity_id,
    JSON_OBJECT(
        'timestamp'         VALUE n.`timestamp`,
        'flightNumber'      VALUE n.flightNumber,
        'passengerNumber'   VALUE n.passengerNumber,
        'message'           VALUE n.message,
        'name'              VALUE p.`name`,
        'loyaltyStatus'     VALUE p.loyaltyStatus,
        'email'             VALUE p.email,
        'phone'             VALUE p.phone,
        'contactPreference' VALUE p.contactPreference
    ) AS payload_json
FROM raw_passenger_notifications AS n
LEFT JOIN csv_passengers_notif AS p ON n.passengerNumber = p.passengerId;
