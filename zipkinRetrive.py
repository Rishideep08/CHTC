import requests

ZIPKIN_API_ENDPOINT = "http://localhost:9411/api/v2/traces"

def get_traces(service_name, limit=10):
    headers = {
        "Content-Type": "application/json",
    }

    params = {
        "serviceName": service_name,
        "limit": limit,
    }

    response = requests.get(ZIPKIN_API_ENDPOINT, headers=headers, params=params)

    if response.status_code == 200:
        traces = response.json()
        return traces
    else:
        print(f"Failed to retrieve traces. Status code: {response.status_code}")
        print(response.text)
        return []

if __name__ == "__main__":
    # Replace 'your_service_name' with the actual service name you want to retrieve traces for
    service_name = "zipkin_service_4"
    traces = get_traces(service_name)

    if traces:
        print("Retrieved traces:")
        for trace in traces:
            print(trace)
    else:
        print("No traces found.")
