import base64
import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_CATEGORY_FIELDS = (
    "category_id",
    "category_image",
    "category_title",
    "category_description",
    "category_section",
    "category_enabled",
    "category_date",
)

UPDATABLE_CATEGORY_FIELDS = (
    "category_image",
    "category_hero",
    "category_title",
    "category_description",
    "category_section",
    "category_players",
    "category_details",
    "category_variations",
    "category_addons",
    "category_information",
    "category_programs",
    "category_category",
    "category_apply",
    "category_enabled",
    "category_status",
    "category_date",
    "category_tags",
)

LIST_FIELD_SCHEMAS = {
    "category_hero": ("image_url", "image_text"),
    "category_players": (
        "player_image",
        "player_says",
        "player_description",
    ),
    "category_details": (
        "detail_title",
        "detail_text",
        "detail_file",
    ),
    "category_variations": (
        "variations_description",
        "variations_dates",
        "variations_cost",
        "variations_deadline",
    ),
    "category_addons": (
        "addons_title",
        "addons_description",
        "addons_cost",
    ),
    "category_information": (
        "information_title",
        "information_image",
        "information_url",
    ),
    "category_programs": (
        "program_id",
        "program_order",
        "program_status",
    ),
}

CATEGORY_DEFAULT_FIELDS = {
    "category_hero": [],
    "category_players": [],
    "category_details": [],
    "category_variations": [],
    "category_addons": [],
    "category_information": [],
    "category_programs": [
        {
            "program_id": "",
            "program_order": "1",
            "program_status": True,
        }
    ],
    "category_category": "",
    "category_apply": "",
    "category_status": "",
    "category_tags": "",
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


def _get_http_method(event, default="GET"):
    try:
        outer_body = _get_body_payload(event)

        if isinstance(outer_body, dict):
            nested_method = outer_body.get("httpMethod")

            if isinstance(nested_method, str) and nested_method.strip():
                return nested_method.strip().upper()

    except Exception:
        pass

    method = event.get("httpMethod") or default
    return method.upper()


def _get_path_parameters(event):
    try:
        outer_body = _get_body_payload(event)

        if isinstance(outer_body, dict):
            nested_path_parameters = outer_body.get("pathParameters")

            if isinstance(nested_path_parameters, dict):
                return nested_path_parameters

    except Exception:
        pass

    path_parameters = event.get("pathParameters")
    return path_parameters if isinstance(path_parameters, dict) else {}


def _get_body_payload(event):
    """
    Soporta:
    1. API Gateway REST/HTTP API:
       event["body"] = '{"category_id":"1", ...}'

    2. Lambda Console directo:
       {
         "category_id": "1",
         ...
       }
    """

    body = event.get("body")

    if body is None:
        return event

    if event.get("isBase64Encoded"):
        if not isinstance(body, str):
            raise ValueError("El body en base64 debe ser texto.")

        try:
            body = base64.b64decode(body).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError("El body en base64 no es valido.") from error

    if isinstance(body, dict):
        return body

    if not isinstance(body, str):
        raise ValueError("El body debe ser texto JSON.")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError("El body no contiene un JSON valido.") from error


def _get_table():
    table_name = os.environ.get("CATEGORY_TABLE_NAME", "category")
    if not table_name:
        raise ValueError("Falta la variable de entorno CATEGORY_TABLE_NAME.")
    return dynamodb.Table(table_name)


def _parse_body(event):
    outer_body = _get_body_payload(event)

    if outer_body is None:
        return {}

    if not isinstance(outer_body, dict):
        raise ValueError("El body externo debe ser un objeto JSON.")

    inner_body = outer_body.get("body")

    if isinstance(inner_body, str):
        try:
            payload = json.loads(inner_body)
        except json.JSONDecodeError as error:
            raise ValueError("El body interno no contiene JSON valido.") from error

        if not isinstance(payload, dict):
            raise ValueError("El body interno debe ser un objeto JSON.")

        return payload

    if isinstance(inner_body, dict):
        return inner_body

    return outer_body


def _validate_required_fields(body):
    missing_fields = [field for field in REQUIRED_CATEGORY_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_category_field_types(body)


def _build_category_item(body):
    category = {
        field: [dict(item) if isinstance(item, dict) else item for item in value]
        if isinstance(value, list)
        else value
        for field, value in CATEGORY_DEFAULT_FIELDS.items()
    }
    category.update({field: body[field] for field in REQUIRED_CATEGORY_FIELDS})

    for field in CATEGORY_DEFAULT_FIELDS:
        if field in body:
            category[field] = body[field]

    return category


def _normalize_category_item(category):
    for field, value in CATEGORY_DEFAULT_FIELDS.items():
        if field in category:
            continue

        category[field] = (
            [dict(item) if isinstance(item, dict) else item for item in value]
            if isinstance(value, list)
            else value
        )

    return category


def _validate_category_field_types(fields):
    if "category_enabled" in fields and not isinstance(
        fields["category_enabled"], bool
    ):
        raise ValueError("El campo category_enabled debe ser booleano.")

    if "category_section" in fields and not isinstance(
        fields["category_section"], bool
    ):
        raise ValueError("El campo category_section debe ser booleano.")

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


def _create_category(table, body):
    _validate_required_fields(body)

    category = _build_category_item(body)

    table.put_item(
        Item=category,
        ConditionExpression="attribute_not_exists(category_id)",
    )

    return _build_response(
        201,
        {
            "message": "Categoria creada correctamente.",
            "category": category,
        },
    )


def _update_category(table, category_id, body):
    if not category_id:
        raise ValueError("Debes enviar category_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_CATEGORY_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_category_field_types(update_fields)

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
        Key={"category_id": category_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(category_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Categoria actualizada correctamente.",
            "category": _normalize_category_item(response.get("Attributes", {})),
        },
    )


def _get_category(table, category_id):
    response = table.get_item(Key={"category_id": category_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Categoria no encontrada.", "category_id": category_id},
        )

    return _build_response(
        200,
        {
            "message": "Categoria obtenida correctamente.",
            "category": _normalize_category_item(item),
        },
    )


def _list_categories(table):
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
            "message": "Categorias obtenidas correctamente.",
            "count": len(items),
            "categories": [_normalize_category_item(item) for item in items],
        },
    )


def lambda_handler(event, context):
    method = _get_http_method(event)
    category_id = _get_path_parameters(event).get("category_id")

    try:
        table = _get_table()

        if method == "GET":
            if category_id:
                return _get_category(table, category_id)
            return _list_categories(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_category(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_category(table, category_id, body)

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
                    {"message": "Ya existe una categoria con ese category_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {"message": "Categoria no encontrada.", "category_id": category_id},
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
