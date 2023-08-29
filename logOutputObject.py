class EventObject:
    def __init__(self, cluster, proc, timestamp, event_type):
        self.cluster = cluster
        self.proc = proc
        self.timestamp = timestamp
        self.event_type = event_type

    def __str__(self):
        return f"Cluster: {self.cluster}, Process: {self.proc}, Timestamp: {self.timestamp}, Event Type: {self.event_type}"


def parse_event_line(line):
    # Split the line and extract the attributes
    cluster, proc, timestamp, event_type = line.strip().split(',')
    return EventObject(cluster, proc, timestamp, event_type)


def read_events_from_file(filename):
    events = []
    with open(filename, 'r') as file:
        for line in file:
            event = parse_event_line(line)
            events.append(event)
    return events


# Example usage:
if __name__ == "__main__":
    filename = "events.txt"  # Replace this with the path to your file
    events_list = read_events_from_file(filename)

    for event in events_list:
        print(event)
