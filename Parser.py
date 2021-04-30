def parse_bus(response):
    lookup = {#'route_long':['LineRef'],
              #'direction':['DirectionRef'],
              'service_date': ['FramedVehicleJourneyRef', 'DataFrameRef'],
              'trip_id': ['FramedVehicleJourneyRef', 'DatedVehicleJourneyRef'],
              #'gtfs_shape_id': ['JourneyPatternRef'],
              #'route_short': ['PublishedLineName'],
              #'agency': ['OperatorRef'],
              #'origin_id':['OriginRef'],
              #'destination_id':['DestinationRef'],
              #'destination_name':['DestinationName'],
              'next_stop_id': ['MonitoredCall','StopPointRef'], #<-- GTFS of next stop
              'next_stop_eta': ['MonitoredCall','ExpectedArrivalTime'], # <-- eta to next stop
              'next_stop_d_along_route': ['MonitoredCall','Extensions','Distances','CallDistanceAlongRoute'], # <-- The distance of the stop from the beginning of the trip/route
              'next_stop_d': ['MonitoredCall','Extensions','Distances','DistanceFromCall'], # <-- The distance of the stop from the beginning of the trip/route
              #'alert': ['SituationRef', 'SituationSimpleRef'],
              #'lat':['VehicleLocation','Latitude'],
              #'lon':['VehicleLocation','Longitude'],
              #'bearing': ['Bearing'],
              #'progress_rate': ['ProgressRate'],
              #'progress_status': ['ProgressStatus'],
              #'occupancy': ['Occupancy'],
              'vehicle_id':['VehicleRef'], #use this to lookup if articulated or not https://en.wikipedia.org/wiki/MTA_Regional_Bus_Operations_bus_fleet
              #'gtfs_block_id':['BlockRef'],
              'passenger_count': ['MonitoredCall', 'Extensions','Capacities','EstimatedPassengerCount']
              }
    bus = dict()
    try:
        for b in response['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:

            for k,v in lookup.items():
                try:
                    if len(v) == 2:
                        val = b['MonitoredVehicleJourney'][v[0]][v[1]]
                        setattr(bus, k, val)
                    elif len(v) == 4:
                        val = b['MonitoredVehicleJourney'][v[0]][v[1]][v[2]][v[3]]
                        setattr(bus, k, val)
                    else:
                        val = b['MonitoredVehicleJourney'][v[0]]
                        setattr(bus, k, val)
                except LookupError:
                    pass
                except Exception as e:
                    pass

    except KeyError: #no VehicleActivity?
        pass
    return bus
