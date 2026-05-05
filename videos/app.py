import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_VIDEO_FIELDS = (
    "video_id",
    "video_title",
    "video_url",
    "video_alt",
    "video_date",
    "video_order",
    "video_enabled",
)

UPDATABLE_VIDEO_FIELDS = (
    "video_title",
    "video_url",
    "video_alt",
    "video_date",
    "video_order",
    "video_enabled",
)


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


def _get_table():
    table_name = os.environ.get("VIDEOS_TABLE_NAME", "videos")
    if not table_name:
        raise ValueError("Falta la variable de entorno VIDEOS_TABLE_NAME.")
    return dynamodb.Table(table_name)


def _parse_body(event):
    body = event.get("body")

    if body is None:
        return {}

    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            raise ValueError("El body debe ser un JSON valido.")

    if not isinstance(body, dict):
        raise ValueError("El body debe ser un objeto JSON.")

    return body


def _validate_required_fields(body):
    missing_fields = [field for field in REQUIRED_VIDEO_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_video_field_types(body)


def _validate_video_field_types(fields):
    if "video_enabled" in fields and not isinstance(fields["video_enabled"], bool):
        raise ValueError("El campo video_enabled debe ser booleano.")

    if "video_order" in fields and (
        not isinstance(fields["video_order"], int)
        or isinstance(fields["video_order"], bool)
    ):
        raise ValueError("El campo video_order debe ser numerico entero.")


def _create_video(table, body):
    _validate_required_fields(body)

    video = {
        "video_id": body["video_id"],
        "video_title": body["video_title"],
        "video_url": body["video_url"],
        "video_alt": body["video_alt"],
        "video_date": body["video_date"],
        "video_order": body["video_order"],
        "video_enabled": body["video_enabled"],
    }

    table.put_item(
        Item=video,
        ConditionExpression="attribute_not_exists(video_id)",
    )

    return _build_response(
        201,
        {
            "message": "Video creado correctamente.",
            "video": video,
        },
    )


def _update_video(table, video_id, body):
    if not video_id:
        raise ValueError("Debes enviar video_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_VIDEO_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_video_field_types(update_fields)

    expression_attribute_names = {}
    expression_attribute_values = {}
    update_parts = []

    for index, (field_name, field_value) in enumerate(update_fields.items(), start=1):
        name_key = f"#field{index}"
        value_key = f":value{index}"
        expression_attribute_names[name_key] = field_name
        expression_attribute_values[value_key] = field_value
        update_parts.append(f"{name_key} = {value_key}")

    response = table.update_item(
        Key={"video_id": video_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(video_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Video actualizado correctamente.",
            "video": response.get("Attributes", {}),
        },
    )


def _get_video(table, video_id):
    response = table.get_item(Key={"video_id": video_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Video no encontrado.", "video_id": video_id},
        )

    return _build_response(
        200,
        {
            "message": "Video obtenido correctamente.",
            "video": item,
        },
    )


def _list_videos(table):
    items = []
    scan_kwargs = {}

    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

        scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

    return _build_response(
        200,
        {
            "message": "Videos obtenidos correctamente.",
            "count": len(items),
            "videos": items,
        },
    )


def lambda_handler(event, context):
    method = (event.get("httpMethod") or "GET").upper()
    video_id = (event.get("pathParameters") or {}).get("video_id")

    try:
        table = _get_table()

        if method == "GET":
            if video_id:
                return _get_video(table, video_id)
            return _list_videos(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_video(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_video(table, video_id, body)

        return _build_response(
            405,
            {"message": f"Metodo {method} no soportado."},
        )
    except ValueError as error:
        return _build_response(400, {"message": str(error)})
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            if method == "POST":
                return _build_response(
                    409,
                    {"message": "Ya existe un video con ese video_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {"message": "Video no encontrado.", "video_id": video_id},
                )

        return _build_response(
            500,
            {
                "message": "Error consultando DynamoDB.",
                "details": str(error),
            },
        )
    except BotoCoreError as error:
        return _build_response(
            500,
            {
                "message": "Error consultando DynamoDB.",
                "details": str(error),
            },
        )
