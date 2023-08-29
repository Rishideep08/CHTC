import requests
import time
import random
import uuid


ZIPKIN_ENDPOINT = "http://localhost:9411/api/v2/spans"

def generate_trace_id():
    # Generate a random UUID and convert it to a 32-character lowercase hexadecimal string
    return uuid.uuid4().hex[:16]


def send_trace(service_name, operation_name, trace_id, parent_span_id, span_id, is_sampled,timestamp_micros,jobId):
    headers = {
        "Content-Type": "application/json",
    }
    is_sampled_str = str(is_sampled).lower()


    # data = [{'traceId': '14e68e86d42256a74c9cd9fa8dbac261', 'id': 'f36d1ab7313ef75b', 'name': 'schedd_start_span_1', 'timestamp': 1691176228424240, 'duration': 544, 'localEndpoint': {'serviceName': service_name, 'port': 9411}, 'tags': {'JobId : ': '30', 'Test Schedd span exporter :': 'Rishideep Schedd_v1', 'otel.library.name': 'Claim tracer', 'otel.library.version': ''}}]
    # data = [span]

    # data = [{'traceId': trace_id, 'id': span_id, 'name': 'schedd_start_span_2', 'timestamp': timestamp_micros, 'duration': 544, 'localEndpoint': {'serviceName': service_name, 'port': 9411}, 'tags': {'JobId : ': jobId, 'Test Schedd span exporter :': 'Rishideep Schedd_v1', 'otel.library.name': 'Claim tracer', 'otel.library.version': ''}}]

    data = [
        {
            'traceId': trace_id,
            'id': span_id,
            'parentId' : parent_span_id,
            'name': 'schedd_start_span_2',
            'timestamp': timestamp_micros,
            'duration': 544,
            'localEndpoint': {
                'serviceName': service_name,
                'port': 9411,
            },
            'tags': {
                'JobId': jobId,
                'Test Schedd span exporter': 'Rishideep Schedd_v1',
                'otel.library.name': 'Claim tracer',
                'otel.library.version': '',
            }
        }
    ]


    response = requests.post(ZIPKIN_ENDPOINT,json=data,headers=headers)

    # print(response.status_code)

    if response.status_code == 202:
        print("Trace sent successfully!")
    else:
        print(f"Failed to send trace. Status code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    service_name = "zipkin_service_8"
    operation_name = "zipkin_operation"
    trace_id = "a38adc9005634f1d"
    parent_span_id = "dbe686c0ad124817"
    span_id = generate_trace_id()
    timestamp_micros = int(time.time() * 1000000)
    jobId = "zipkin_job_id"
    is_sampled = True

    send_trace(service_name, operation_name, trace_id, parent_span_id, span_id, is_sampled,timestamp_micros,jobId)
