from minio import Minio # Import Minio library
import os # Import os library
from datetime import datetime # Import datetime library

# Function to create a client
def f_create_client(SERVER_IP, SERVER_PORT, ACCESS_KEY, SECRET_KEY):
    return Minio(SERVER_IP+":"+SERVER_PORT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False) # Return client

# Function to check if bucket exists
def f_does_bucket_exist(client, bucket_name):
    return client.bucket_exists(bucket_name) # Return True if bucket exists, else return False

# Function to check if bucket is empty
def f_is_bucket_empty(client, bucket_name):
    for files in client.list_objects(bucket_name, recursive=True): # Loop through all files in bucket
        return False # Return False if bucket is not empty
    return True # Return True if bucket is empty

# Function to check if file name exists
def f_does_file_name_exist(client, bucket_name, file_name):
    for files in client.list_objects(bucket_name, recursive=True): # Loop through all files in bucket
        if files.object_name == file_name: # Check if file name exists
            return True # Return True if file name exists
    return False # Return False if file name does not exist

# Function to upload file to bucket
def f_upload_files(client, bucket_name, pickup_dir):
    if f_does_bucket_exist(client, bucket_name): # Check if bucket exists
        print("Bucket exists")
        if f_is_bucket_empty(client, bucket_name): # Check if bucket is empty
            print("Bucket is empty")
            for file in os.listdir(pickup_dir): # Loop through all files in pickup directory
                try: # Try to upload file
                    client.fput_object(bucket_name, file, os.path.join(pickup_dir, file)) # Upload file
                    print("File: "+file+" uploaded")
                except: # If file upload fails
                    print("File: " + file + " failed to upload")
        else: # If bucket is not empty
            print("Bucket is not empty")
            for file in os.listdir(pickup_dir): # Loop through all files in pickup directory
                if f_does_file_name_exist(client, bucket_name, file): # Check if file name exists
                    print("File name exists")
                    new_file = os.path.splitext(file)[0] + "_"+ datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + os.path.splitext(file)[1] # Create new file name
                    print("File name changed to: " + new_file)
                    try: # Try to upload file
                        client.fput_object(bucket_name, new_file, pickup_dir+"/"+file) # Upload file
                        print("File: " + new_file + " uploaded")
                    except: # If file upload fails
                        print("File: " + new_file + " failed to upload")
                else: # If file name does not exist
                    print("File name does not exist")
                    try: # Try to upload file
                        client.fput_object(bucket_name, file, pickup_dir+"/"+file)  # Upload file
                        print("File: " + file + " uploaded")
                    except: # If file upload fails
                        print("File: " + file + " failed to upload")
    else: # If bucket does not exist
        print("Bucket does not exist")
    
# Main function
def main():

    ACCESS_KEY="" # Access key of the Minio server
    SECRET_KEY="" # Secret key of the Minio server
    SERVER_IP="" # Minio server IP address
    SERVER_PORT="" # Minio server port
    BUCKET_NAME="" # Bucket name
    PICKUP_DIR="" # Directory to upload files from
    client = f_create_client(SERVER_IP, SERVER_PORT, ACCESS_KEY, SECRET_KEY) # Create client
    f_upload_files(client, BUCKET_NAME, PICKUP_DIR) # Upload file to bucket

if __name__ == "__main__":
    main() # Run main function
