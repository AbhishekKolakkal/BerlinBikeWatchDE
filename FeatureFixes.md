### Features
1. Right now we are passing both places and individual bike stations but not working on individual bike station, that part will be done afterwards
2. Build Terraform Script after the whole project is completed 
3. After completing the project with places, we will need to also add the non places spots also and that data is also a lot.
4. Adding more data
- non places - after doing some analysis of current data
- bike list - 


### Fixes
1. Need to add try/except for network error handling if there is any hiccup while polling the api



with the current system I can get
1. Anomaly detection
2. dead Station
3. how many stations are in maintenance mode
4. Busiest / quietest stations — rank stations by average bikes_available_to_rent, or by how often they hit zero, or by variance (how much their count fluctuates — a station that's always near-empty is different from one that swings wildly)
5. Time-of-day demand patterns — group by EXTRACT(hour FROM fetched_at) per station, revealing rush-hour signatures (e.g., "this station empties every morning around 8am, refills around 6pm")
8. Rebalancing candidates — stations that are consistently full (high bikes, low free_racks) paired with stations consistently empty — a real operational insight (nextbike/Berlin would want to move bikes from the first group to the second)
11. Rack capacity utilization — bikes / bike_racks as a ratio, per station, showing which stations are chronically over- or under-provisioned relative to their physical size