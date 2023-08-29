import htcondor
from htcondor import JobEventType
import requests
import time
import random
import uuid

class EventObject:
    def __init__(self, cluster, proc, timestamp, duration,type,event_type):
        self.cluster = cluster
        self.proc = proc
        self.timestamp = timestamp
        self.event_type = event_type
        self.duration = duration
        self.type = type
        # self.parent_span_id = parent_span_id

    def __str__(self):
        return f"Cluster: {self.cluster}, Process: {self.proc}, Timestamp: {self.timestamp}, Duration: {self.duration} ,Type : {self.type}, Event_type : {self.event_type}"

if __name__ == "__main__":
    jel = htcondor.JobEventLog("EventLog")
    events_list  = []
    count =0
    unique_event_types = []
    for event in jel.events(stop_after=0):
        # print(count)
        # count+=1
        if(event.type not in unique_event_types):
            # print(event.type)
            unique_event_types.append(event.type)
        events_list.append(EventObject(event.cluster,event.proc,"0","0","Submit Event",event.type))

    # for event in events_list:
    #     print(event)
    
    # print(unique_event_types)
    for event in unique_event_types:
        print(event)