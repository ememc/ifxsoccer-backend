import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb")


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def _build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def lambda_handler(event, context):
    program_id = (event.get("pathParameters") or {}).get("program_id")

    if not program_id:
        return _build_response(
            400,
            {"message": "Debes enviar program_id en la URL."},
        )

    table_name = os.environ.get("PROGRAM_TABLE_NAME")
    if not table_name:
        return _build_response(
            500,
            {"message": "Falta la variable de entorno PROGRAM_TABLE_NAME."},
        )

    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={"program_id": program_id})
    except (ClientError, BotoCoreError) as error:
        return _build_response(
            500,
            {
                "message": "Error consultando DynamoDB.",
                "details": str(error),
            },
        )

    item = response.get("Item")
    if not item:
        return _build_response(
            404,
            {"message": "Programa no encontrado.", "program_id": program_id},
        )

    return _build_response(
        200,
        {
            "message": "Programa obtenido correctamente.",
            "program": item,
        },
    )
