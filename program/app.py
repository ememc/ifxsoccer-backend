import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_PROGRAM_FIELDS = (
    "program_id",
    "program_hero",
    "program_title",
    "program_description",
    "program_section",
    "program_players",
    "program_details",
    "program_variations",
    "program_addons",
    "program_information",
    "program_category",
    "program_apply",
    "program_enabled",
    "program_status",
    "program_date",
)

UPDATABLE_PROGRAM_FIELDS = (
    "program_hero",
    "program_title",
    "program_description",
    "program_section",
    "program_players",
    "program_details",
    "program_variations",
    "program_addons",
    "program_information",
    "program_category",
    "program_apply",
    "program_enabled",
    "program_status",
    "program_date",
)

LIST_FIELD_SCHEMAS = {
    "program_hero": ("image_url", "image_text"),
    "program_section": (
        "section_image",
        "section_title",
        "section_text",
        "section_order",
    ),
    "program_players": (
        "player_image",
        "player_says",
        "player_description",
    ),
    "program_details": (
        "detail_title",
        "detail_text",
        "detail_file",
    ),
    "program_variations": (
        "variations_description",
        "variations_dates",
        "variations_cost",
        "variations_deadline",
    ),
    "program_addons": (
        "addons_title",
        "addons_description",
        "addons_cost",
    ),
    "program_information": (
        "information_title",
        "information_image",
        "information_url",
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
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _get_table():
    table_name = os.environ.get("PROGRAM_TABLE_NAME", "programs")
    if not table_name:
        raise ValueError("Falta la variable de entorno PROGRAM_TABLE_NAME.")
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
    missing_fields = [field for field in REQUIRED_PROGRAM_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_program_field_types(body)


def _validate_program_field_types(fields):
    if "program_enabled" in fields and not isinstance(
        fields["program_enabled"], bool
    ):
        raise ValueError("El campo program_enabled debe ser booleano.")

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


def _create_program(table, body):
    _validate_required_fields(body)

    program = {field: body[field] for field in REQUIRED_PROGRAM_FIELDS}

    table.put_item(
        Item=program,
        ConditionExpression="attribute_not_exists(program_id)",
    )

    return _build_response(
        201,
        {
            "message": "Programa creado correctamente.",
            "program": program,
        },
    )


def _update_program(table, program_id, body):
    if not program_id:
        raise ValueError("Debes enviar program_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_PROGRAM_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_program_field_types(update_fields)

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
        Key={"program_id": program_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(program_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Programa actualizado correctamente.",
            "program": response.get("Attributes", {}),
        },
    )


def _get_program(table, program_id):
    response = table.get_item(Key={"program_id": program_id})
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


def _list_programs(table):
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
            "message": "Programas obtenidos correctamente.",
            "count": len(items),
            "programs": items,
        },
    )


def lambda_handler(event, context):
    method = (event.get("httpMethod") or "GET").upper()
    program_id = (event.get("pathParameters") or {}).get("program_id")

    try:
        table = _get_table()

        if method == "GET":
            if program_id:
                return _get_program(table, program_id)
            return _list_programs(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_program(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_program(table, program_id, body)

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
                    {"message": "Ya existe un programa con ese program_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {"message": "Programa no encontrado.", "program_id": program_id},
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
