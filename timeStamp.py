import datetime

# Given timestamp in microseconds
timestamp_micros = 1691178456

# Convert the timestamp to seconds
timestamp_seconds = timestamp_micros / 1000000

# Convert the timestamp to a human-readable date and time
datetime_utc = datetime.datetime.utcfromtimestamp(timestamp_seconds)

print(datetime_utc)
