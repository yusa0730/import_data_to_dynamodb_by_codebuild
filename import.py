import boto3
import json
import decimal
import os

region = os.environ.get('REGION', 'ap-northeast-1');
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1');
table_name = os.environ.get('TABLE_NAME')
table = dynamodb.Table(table_name);

def import_data_from_json_file(file_path):
    with open(file_path, "r") as json_file:
        loaded_data = json.load(json_file)
        data = loaded_data["Items"]
        with table.batch_writer() as batch:
            for row in data:
                message = row['message']['S']
                user_id = int(row['user_id']['N'])
                created_at = row['created_at']['S']
                post_id = int(row['post_id']['N'])
                
                print("Adding data from file:", file_path, "| Post ID:", post_id)

                batch.put_item(
                    Item={
                        'message': message,
                        'user_id': user_id,
                        'created_at': created_at,
                        'post_id': post_id
                    }
                )

if __name__=='__main__':
    dir_path = 'json'
    files = [os.path.join(dir_path, filename) for filename in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, filename)) and filename.endswith('.json')]
    
    for file_path in files:
        import_data_from_json_file(file_path)
