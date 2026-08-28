import requests, json

resp = requests.get("https://maps2.nextbike.net/maps/nextbike-live.json?city=362")
data = resp.json()

with open("nextbike_sample.json", "w") as f:
  json.dump(data, f, indent=2)
