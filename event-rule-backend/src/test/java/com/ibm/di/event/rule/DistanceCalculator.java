package com.ibm.di.event.rule;

/**
 * Calculator to compute the 3D straight-line distance between two geographic points
 * using Latitude, Longitude (in degrees), and Elevation (in meters).
 */
public class DistanceCalculator {

    public static final Position CDG_POS = new Position("CDG", 49.0097, 2.5478, 119);
    public static final Position NCE_POS = new Position("NCE", 43.6584, 7.2159, 4);
    public static final Position JFK_POS = new Position("JFK", 40.6413, -73.7781, 13);
    public static final Position ATL_POS = new Position("ATL", 33.6407, -84.4277, 313);

    public record Position(String name, double latitude, double longitude, double elevation) {
        public Position(double latitude, double longitude, double elevation) {
            this("", latitude, longitude, elevation);
        }
    }

    // Earth's radius in meters (WGS-84 average)
    private static final double EARTH_RADIUS_METERS = 6371000.0;

    /**
     * Calculates the 3D distance between two points.
     * 
     * @param lat1 Lat of point 1 (degrees)
     * @param lon1 Lon of point 1 (degrees)
     * @param alt1 Elevation of point 1 (meters)
     * @param lat2 Lat of point 2 (degrees)
     * @param lon2 Lon of point 2 (degrees)
     * @param alt2 Elevation of point 2 (meters)
     * @param useEcef Set to true for long-distance ECEF accuracy, false for Haversine+Pythagoras
     * @return Total 3D distance in meters
     */
    public static double calculate3DDistance(Position p1, Position p2, boolean useEcef) {
        return calculate3DDistance(p1.latitude(), p1.longitude(), p1.elevation(),
                                  p2.latitude(), p2.longitude(), p2.elevation(),
                                  useEcef);
    }

    public static double calculate3DDistance(Position p1, Position p2) {
        return calculate3DDistance(p1, p2, true);
    }

    /**
     * Calculates the 3D distance between two points.
     *
     * @param lat1 Lat of point 1 (degrees)
     * @param lon1 Lon of point 1 (degrees)
     * @param alt1 Elevation of point 1 (meters)
     * @param lat2 Lat of point 2 (degrees)
     * @param lon2 Lon of point 2 (degrees)
     * @param alt2 Elevation of point 2 (meters)
     * @param useEcef Set to true for long-distance ECEF accuracy, false for Haversine+Pythagoras
     * @return Total 3D distance in meters
     */
    public static double calculate3DDistance(double lat1, double lon1, double alt1,
                                             double lat2, double lon2, double alt2,
                                             boolean useEcef) {
        if (useEcef) {
            return calculateEcefDistance(lat1, lon1, alt1, lat2, lon2, alt2);
        } else {
            return calculateHaversinePythagorasDistance(lat1, lon1, alt1, lat2, lon2, alt2);
        }
    }

    /**
     * Method 1: Haversine + Pythagoras (Best for short distances)
     */
    private static double calculateHaversinePythagorasDistance(double lat1, double lon1, double alt1, 
                                                               double lat2, double lon2, double alt2) {
        // Convert degrees to radians
        double lat1Rad = Math.toRadians(lat1);
        double lon1Rad = Math.toRadians(lon1);
        double lat2Rad = Math.toRadians(lat2);
        double lon2Rad = Math.toRadians(lon2);

        // Haversine formula for surface distance
        double dLat = lat2Rad - lat1Rad;
        double dLon = lon2Rad - lon1Rad;

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
                   
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double surfaceDistance = EARTH_RADIUS_METERS * c;

        // Elevation difference
        double deltaElevation = alt2 - alt1;

        // 3D Pythagoras
        return Math.sqrt(Math.pow(surfaceDistance, 2) + Math.pow(deltaElevation, 2));
    }

    /**
     * Method 2: ECEF Coordinates (Best for long distances / high precision)
     */
    private static double calculateEcefDistance(double lat1, double lon1, double alt1, 
                                                 double lat2, double lon2, double alt2) {
        // Convert to radians
        double lat1Rad = Math.toRadians(lat1);
        double lon1Rad = Math.toRadians(lon1);
        double lat2Rad = Math.toRadians(lat2);
        double lon2Rad = Math.toRadians(lon2);

        // WGS-84 ellipsoid constants
        double a = 6378137.0; // semi-major axis
        double f = 1.0 / 298.257223563; // flattening
        double e2 = 2 * f - f * f; // eccentricity squared

        // Point 1 ECEF
        double N1 = a / Math.sqrt(1 - e2 * Math.sin(lat1Rad) * Math.sin(lat1Rad));
        double x1 = (N1 + alt1) * Math.cos(lat1Rad) * Math.cos(lon1Rad);
        double y1 = (N1 + alt1) * Math.cos(lat1Rad) * Math.sin(lon1Rad);
        double z1 = (N1 * (1 - e2) + alt1) * Math.sin(lat1Rad);

        // Point 2 ECEF
        double N2 = a / Math.sqrt(1 - e2 * Math.sin(lat2Rad) * Math.sin(lat2Rad));
        double x2 = (N2 + alt2) * Math.cos(lat2Rad) * Math.cos(lon2Rad);
        double y2 = (N2 + alt2) * Math.cos(lat2Rad) * Math.sin(lon2Rad);
        double z2 = (N2 * (1 - e2) + alt2) * Math.sin(lat2Rad);

        // Straight-line 3D distance
        return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2) + Math.pow(z2 - z1, 2));
    }

    public static void main(String[] args) {
        Position[] airports = { CDG_POS, NCE_POS, JFK_POS, ATL_POS };

        System.out.println("=== Airport Distances ===");
        for (int i = 0; i < airports.length; i++) {
            for (int j = i + 1; j < airports.length; j++) {
                Position p1 = airports[i];
                Position p2 = airports[j];

                double distEcef = calculate3DDistance(p1, p2, true);
                double distHaversine = calculate3DDistance(p1, p2, false);

                System.out.printf("%s -> %s:%n", p1.name(), p2.name());
                System.out.printf("  ECEF (straight-line 3D):      %10.2f m  (%8.2f km)%n", distEcef, distEcef / 1000.0);
                System.out.printf("  Haversine + Pythagoras:       %10.2f m  (%8.2f km)%n", distHaversine, distHaversine / 1000.0);
            }
        }
    }
}
