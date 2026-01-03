import json
import boto3
import io
import os
from PIL import Image

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        bucket = body['bucket']
        key = body['key'] # Ejemplo: incoming/foto.jpg
        
        # Definir ruta de salida (Regla 5)
        filename = key.split('/')[-1]
        metadata_key = f"metadata/{filename}.json"
        
        # Idempotencia: Verificar si ya existe
        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            print(f"Metadata for {key} already exists. Skipping.")
            continue
        except:
            pass # No existe, procedemos

        # Procesamiento
        response = s3.get_object(Bucket=bucket, Key=key)
        img_bytes = response['Body'].read()
        
        with Image.open(io.BytesIO(img_bytes)) as img:
            metadata = {
                "source_bucket": bucket,
                "source_key": key,
                "format": img.format,
                "size_px": img.size,
                "mode": img.mode,
                "file_size": len(img_bytes),
                "exif": str(img.info.get('exif', 'No EXIF'))
            }

        # Guardar en prefijo metadata/
        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType='application/json'
        )
        print(f"Metadata saved to {metadata_key}")