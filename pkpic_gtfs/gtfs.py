# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon", "platform_code"),
    "routes.txt": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
        "route_sort_order",
    ),
    "trips.txt": (
        "route_id",
        "trip_id",
        "service_id",
        "trip_headsign",
        "trip_short_name",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "platform",
        "fare_dist_m",
    ),
    "transfers.txt": (
        "from_stop_id",
        "to_stop_id",
        "from_trip_id",
        "to_trip_id",
        "transfer_type",
    ),
    "calendar.txt": (
        "service_id",
        "start_date",
        "end_date",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ),
}
