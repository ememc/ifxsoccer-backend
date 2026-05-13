import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_DESTINATION_FIELDS = (
    "destination_id",
    "destination_category",
    "destination_date",
    "destination_title",
    "destination_description",
    "destination_hero",
    "destination_section",
    "destination_cities",
    "destination_academies",
    "destination_state",
    "destination_tags",
    "destination_text",
)

UPDATABLE_DESTINATION_FIELDS = (
    "destination_category",
    "destination_date",
    "destination_title",
    "destination_description",
    "destination_hero",
    "destination_section",
    "destination_cities",
    "destination_academies",
    "destination_state",
    "destination_tags",
    "destination_text",
)

LIST_FIELD_SCHEMAS = {
    "destination_hero": ("image_text", "image_url"),
    "destination_section": (
        "section_image",
        "section_order",
        "section_text",
        "section_title",
    ),
    "destination_cities": (
        "city_image",
        "city_order",
        "city_text",
        "city_title",
    ),
    "destination_academies": (
        "academy_image",
        "academy_order",
        "academy_text",
        "academy_title",
        "academy_target",
    ),
}


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
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _get_table():
    table_name = os.environ.get("DESTINATION_TABLE_NAME", "destinations")
    if not table_name:
        raise ValueError("Falta la variable de entorno DESTINATION_TABLE_NAME.")
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
    missing_fields = [field for field in REQUIRED_DESTINATION_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_destination_field_types(body)


def _validate_destination_field_types(fields):
    for field_name, required_keys in LIST_FIELD_SCHEMAS.items():
        if field_name not in fields:
            continue

        items = fields[field_name]
        if not isinstance(items, list):
            raise ValueError(f"El campo {field_name} debe ser una lista.")

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(
                    f"El campo {field_name}[{index}] debe ser un objeto."
                )

            missing_keys = [key for key in required_keys if key not in item]
            if missing_keys:
                raise ValueError(
                    "Faltan campos en "
                    f"{field_name}[{index}]: {', '.join(missing_keys)}."
                )


def _create_destination(table, body):
    _validate_required_fields(body)

    destination = {field: body[field] for field in REQUIRED_DESTINATION_FIELDS}

    table.put_item(
        Item=destination,
        ConditionExpression="attribute_not_exists(destination_id)",
    )

    return _build_response(
        201,
        {
            "message": "Destino creado correctamente.",
            "destination": destination,
        },
    )


def _update_destination(table, destination_id, body):
    if not destination_id:
        raise ValueError("Debes enviar destination_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_DESTINATION_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_destination_field_types(update_fields)

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
        Key={"destination_id": destination_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(destination_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Destino actualizado correctamente.",
            "destination": response.get("Attributes", {}),
        },
    )


def _get_destination(table, destination_id):
    response = table.get_item(Key={"destination_id": destination_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Destino no encontrado.", "destination_id": destination_id},
        )

    return _build_response(
        200,
        {
            "message": "Destino obtenido correctamente.",
            "destination": item,
        },
    )


def _list_destinations(table):
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
            "message": "Destinos obtenidos correctamente.",
            "count": len(items),
            "destinations": items,
        },
    )


def lambda_handler(event, context):
    method = (event.get("httpMethod") or "GET").upper()
    destination_id = (event.get("pathParameters") or {}).get("destination_id")

    try:
        if method == "OPTIONS":
            return _build_response(200, {"message": "CORS preflight OK."})

        table = _get_table()

        if method == "GET":
            if destination_id:
                return _get_destination(table, destination_id)
            return _list_destinations(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_destination(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_destination(table, destination_id, body)

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
                    {"message": "Ya existe un destino con ese destination_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {
                        "message": "Destino no encontrado.",
                        "destination_id": destination_id,
                    },
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
