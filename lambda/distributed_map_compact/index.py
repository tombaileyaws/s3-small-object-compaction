import boto3 
import pathlib
import json
import string
import random
import os

s3 = boto3.client('s3')

def list_objects_in_s3(bucket, prefix):
    contents = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents.extend(response.get('Contents', []))
    
        if 'NextContinuationToken' not in response:
            break
      
        continuation_token = response['NextContinuationToken']

    return contents if contents else 'None'

def get_object_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def upload_object_to_s3(bucket, key, file_path, storage_class):
    s3.upload_file(file_path, bucket, key,ExtraArgs={'StorageClass': storage_class})

def merge_objects_from_s3(items,source_bucket,source_prefix,target_bucket,target_prefix, temp_path, storage_class):
    objects = items
    if objects == 'None':
        return {
        "statusCode": 200,
        "body": "There were no objects to compact!"
    } 
    else:
        out_path = temp_path + target_prefix.replace("/", "-") + objects[0]['Key'].split("/")[-1] + ''.join(pathlib.Path(objects[0]['Key'].split("/")[-1]).suffixes)
        out_key = target_prefix + "/" + target_prefix.replace("/", "-") +  ''.join(pathlib.Path(objects[0]['Key'].split("/")[-1]).suffixes) + "-" + id_generator()
        for index, object in enumerate(objects):
            if object['Size'] > 131072:
                
                large_obj_out_key = target_prefix + "/" + target_prefix.replace("/", "-") +  ''.join(pathlib.Path(objects[0]['Key'].split("/")[-1]).suffixes) + "-" + id_generator()
                s3.copy_object(
                    Bucket=target_bucket,
                    CopySource= source_bucket+"/"+object['Key'],
                    Key=large_obj_out_key,
                    StorageClass=storage_class
                    )
                print(f"Copied object >1MB to {out_key}")
                objects.pop(index)
            key = object['Key']
            data = get_object_from_s3(source_bucket, key)
            with open(out_path, 'ab') as f:
                f.write(data)
        if len(objects) > 0:
            upload_object_to_s3(target_bucket, out_key, out_path, storage_class)
            print(f"Merged {len(objects)} objects into {out_key}")
        
        if os.path.exists(out_path):
            os.remove(out_path)
            print("Removed the file %s" % out_path)     
        else:
            print("Sorry, file %s does not exist." % out_path)

def split_s3_parts(s3_uri):
    path_parts=s3_uri.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def lambda_handler(event, context):
    #print(event)
    s_bucket, s_key = split_s3_parts(json.loads(event['BatchInput']['src']))
    d_bucket, d_key = split_s3_parts(json.loads(event['BatchInput']['dest']))
    storage_class = event['BatchInput']['execution_input'].get('storage_class', 'STANDARD')
    merge_objects_from_s3(event['Items'],s_bucket,s_key,d_bucket,d_key,"/tmp/",storage_class)

    print("Compaction complete!")

    return {
        "statusCode": 200,
        "body": "Compaction complete!"
    }