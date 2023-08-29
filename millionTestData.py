def read_file(file_path):
    with open(file_path, "r") as file:
        content = file.readlines()
    return content

def write_file(file_path, content):
    with open(file_path, "w") as file:
        file.writelines(content)

# Read the content of the original file
original_content = read_file("sleep.log")

# Modify cluster ID and create a new file
new_file_content = []
initial_cluster_id = 1

for i in range(170000):
    print(i)
    updated_content = [line.replace("035.000.000", f"{i+1:03d}.000.000") for line in original_content]
    new_file_content.extend(updated_content)
    i = i+1
    

# Write the new content to the new file
write_file("new_file_1.log", new_file_content)
