### Features

1. Right now we are passing both places and individual bike stations but not working on individual bike station, that part will be done afterwards
2. Build Terraform Script after the whole project is completed 
3. After completing the project with places, we will need to also add the non places spots also and that data is also a lot.
4. Adding more data
- non places - after doing some analysis of current data
- bike list - 
5. on 8th sept the producer was down for an hour and I was not knowing it, right now there is a try catch block but this will not be enough, we needed a mechanism that will give alert to me if any service is down.
14. RAG-style Q&A over your whole project — combine several of the above (SQL queries, field docs, README) into one vector store, and build a small "ask a question about BerlinBikeWatch, get an answer grounded in the actual project" tool — this is genuinely the most "industry-relevant" vector DB pattern right now (retrieval-augmented generation), and it's built entirely from documentation you already have.


### Fixes
1. Need to add try/except for network error handling if there is any hiccup while polling the api



with the current system I can get
1. Anomaly detection
2. dead Station - done
3. how many stations are in maintenance mode
4. Busiest / quietest stations — rank stations by average bikes_available_to_rent, or by how often they hit zero, or by variance (how much their count fluctuates — a station that's always near-empty is different from one that swings wildly) - done
5. Time-of-day demand patterns — group by EXTRACT(hour FROM fetched_at) per station, revealing rush-hour signatures (e.g., "this station empties every morning around 8am, refills around 6pm")
8. Rebalancing candidates — stations that are consistently full (high bikes, low free_racks) paired with stations consistently empty — a real operational insight (nextbike/Berlin would want to move bikes from the first group to the second)
11. Rack capacity utilization — bikes / bike_racks as a ratio, per station, showing which stations are chronically over- or under-provisioned relative to their physical size

