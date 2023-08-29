import htcondor
from htcondor import JobEventType
import requests
import time
import random
import uuid
import os

#start the docker on 9411 port and run this query.

ZIPKIN_ENDPOINT = "http://localhost:9411/api/v2/spans"

def generate_unique_id():
    # Generate a random UUID and convert it to a 32-character lowercase hexadecimal string
    return uuid.uuid4().hex[:16]


def getTimeStamp():
    return int(time.time() * 1000000)

#sending trace for each sub span.
def send_trace(event,trace_id,parent_span_id):
    headers = {
        "Content-Type": "application/json",
    }

    val = event.timestamp*1000000
    span_id = generate_unique_id()

    data = [
        {
            'traceId': trace_id,
            'id':span_id,
            'name': event.type,
            'timestamp': val,
            'localEndpoint': {
                'serviceName':generate_job_id(event.proc,event.cluster),
                'port': 9411,
            },
            'parentId' : parent_span_id,
            'tags': {
                'JobId': event.proc,
                # 'JobType' : event.event_type,
            }
        }
    ]


    response = requests.post(ZIPKIN_ENDPOINT,json=data,headers=headers)

    if response.status_code != 202:
        job_id = generate_job_id(event.proc, event.cluster)
        print(f"Failed to send trace. Status code: {response.status_code}")
        print(response.text)

    return trace_id,span_id


#initial root 
def send_trace_root(filename):
    headers = {
        "Content-Type": "application/json",
    }
    trace_id =  generate_unique_id()
    span_id = generate_unique_id()
    data = [
        {
            'traceId':trace_id,
            'id':span_id,
            'name': "ROOT",
            'localEndpoint': {
                'serviceName':filename,
                'port': 9411,
            },
        }
    ]


    response = requests.post(ZIPKIN_ENDPOINT,json=data,headers=headers)


    if response.status_code == 202:
        print("Trace sent successfully!")
    else:
        print(f"Failed to send trace root. Status code: {response.status_code}")
        print(response.text)

    return trace_id,span_id


class EventObject:
    def __init__(self, cluster, proc, timestamp, duration,type):
        self.cluster = cluster
        self.proc = proc
        self.timestamp = timestamp
        # self.event_type = event_type
        self.duration = duration
        self.type = type
        # self.parent_span_id = parent_span_id

    def __str__(self):
        return f"Cluster: {self.cluster}, Process: {self.proc}, Timestamp: {self.timestamp}, Duration: {self.duration} ,Type : {self.type}"


def generate_job_id(proc, cluster):
    return f"{cluster}.{proc}"

def createJobClusterForZipkin(events,root_trace_id,root_span_id):
    events_list = []
    submit_start_time = -1
    submit_span_id = -1
    submit_trace_id = -1
    file_transfer_state = 0
    input_file_start_time = -1
    output_file_start_time = -1
    execute_end_time = -1
    cluster = -1
    proc = -1
    attempts = {}
    evict_number = 1
    execute_completed = 0
    for event in events:
        #Starting the hierarchy so we are getting the duration b/w the submit and job termination.
        if(event.type == JobEventType.SUBMIT):
            submit_start_time = event.timestamp
            cluster = event.cluster
            proc = event.proc
            attempts[evict_number] = [submit_start_time]

        if(event.type == JobEventType.JOB_TERMINATED):
            if(submit_start_time != -1):
                if(execute_completed == 0):
                    execute_completed = 1
                    attempts[evict_number].append(EventObject(event.cluster,event.proc,execute_start_time,execute_end_time-execute_start_time,"Execute"))
                attempts[evict_number].append(event.timestamp)

                #sending the submit trace.
                submit_trace_id,submit_span_id = send_trace(EventObject(event.cluster,event.proc,submit_start_time,event.timestamp-submit_start_time,"Submit Event"),root_trace_id,root_span_id) 
            else:
                print("No submit event but the event got terminated")
        
        #getting the data for each different type of file transfer and parent span details and sending the new trace.
    
        #for input -> FILETRANSFER START AND FILETRANSFER END
        #for output -> FILETRANSFER START AND FILETRANSFER END
        #so 4 states
    

        if(event.type == JobEventType.FILE_TRANSFER):
            if(file_transfer_state == 0):
                input_file_start_time = event.timestamp
            elif(file_transfer_state == 1):
                if(input_file_start_time!=-1):
                    attempts[evict_number].append(EventObject(event.cluster,event.proc,input_file_start_time,event.timestamp-input_file_start_time,"Input_file_transfer"))
                    # _,_ = send_trace(,submit_trace_id,,submit_span_id)
                else: 
                    print("input file start time is missing")

            elif(file_transfer_state == 2):
                output_file_start_time = event.timestamp
                execute_end_time = output_file_start_time
                if(execute_start_time !=-1):
                    execute_completed = 1
                    attempts[evict_number].append(EventObject(event.cluster,event.proc,execute_start_time,output_file_start_time-execute_start_time,"Execute"))
                else:
                    print("execute_start_time is missing")
            
            elif(file_transfer_state == 3):
                if(output_file_start_time!=-1):
                    attempts[evict_number].append(EventObject(event.cluster,event.proc,output_file_start_time,event.timestamp-output_file_start_time,"Output_file_transfer"))
                else :
                    print("output file start time is missing")

            #it looks into 4 states. 
            file_transfer_state = (file_transfer_state+1)%4; 

        #execute case
        if(event.type == JobEventType.EXECUTE):
            execute_start_time = event.timestamp
            execute_completed = 0

        if(event.type == JobEventType.JOB_EVICTED):
            attempts[evict_number].append(event.timestamp)
            evict_number = evict_number+1
            attempts[evict_number] = [event.timestamp]
            input_file_start_time = -1
            output_file_start_time = -1
            execute_end_time = -1
            execute_start_time = -1
            execute_completed = 0


    #so for each attempt we are creating a new hierarchy and appending to the submit trace.
    for evict_number, attempt in attempts.items():
        attempt_val = "Attempt"+str(evict_number)
        attemp_event = EventObject(cluster,proc,attempt[0],attempt[-1]-attempt[0],attempt_val)
        attempt_trace_id,attempt_span_id = send_trace(attemp_event,submit_trace_id,submit_span_id)
        
        for i in range(1, len(attempt) - 1):
            send_trace(attempt[i],attempt_trace_id,attempt_span_id)
        
     
#we are handling only submit->input filetransfer->execution/job eviction->output filetranfer->job_termination.

if __name__ == "__main__":
    filename = "sleep.log"
    jel = htcondor.JobEventLog(filename)
    job_events = {}
    for event in jel.events(stop_after=0):
        job_id = generate_job_id(event.proc, event.cluster)
        #filter the job events for each job id.
        if job_id in job_events:
            job_events[job_id].append(event)
        else:
            job_events[job_id] = [event]

    #creating a root and all these jobs are present sub directory of it. 
    root_trace_id,root_span_id = send_trace_root(os.path.splitext(filename)[0])


    #we are creating the hierarchy for each job.
    for job_id, events in job_events.items():
        try :
            createJobClusterForZipkin(events,root_trace_id,root_span_id)
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    



    
