import base64
import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_IMAGE_FIELDS = (
    "image_id",
    "image_title",
    "image_url",
    "image_alt",
    "image_date",
    "image_order",
    "image_enabled",
    "image_category",
    "image_tags",
)

UPDATABLE_IMAGE_FIELDS = (
    "image_title",
    "image_url",
    "image_alt",
    "image_date",
    "image_order",
    "image_enabled",
    "image_category",
    "image_tags",
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
       event["body"] = '{"image_id":"1", ...}'

    2. Lambda Console directo:
       {
         "image_id": "1",
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
    table_name = os.environ.get("IMAGES_TABLE_NAME", "images")
    if not table_name:
        raise ValueError("Falta la variable de entorno IMAGES_TABLE_NAME.")
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
    missing_fields = [field for field in REQUIRED_IMAGE_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_image_field_types(body)


def _validate_image_field_types(fields):
    if "image_enabled" in fields and not isinstance(fields["image_enabled"], bool):
        raise ValueError("El campo image_enabled debe ser booleano.")

    if "image_order" in fields and (
        not isinstance(fields["image_order"], int)
        or isinstance(fields["image_order"], bool)
    ):
        raise ValueError("El campo image_order debe ser numerico entero.")


def _create_image(table, body):
    _validate_required_fields(body)

    image = {field: body[field] for field in REQUIRED_IMAGE_FIELDS}

    table.put_item(
        Item=image,
        ConditionExpression="attribute_not_exists(image_id)",
    )

    return _build_response(
        201,
        {
            "message": "Imagen creada correctamente.",
            "image": image,
        },
    )


def _update_image(table, image_id, body):
    if not image_id:
        raise ValueError("Debes enviar image_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_IMAGE_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_image_field_types(update_fields)

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
        Key={"image_id": image_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(image_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Imagen actualizada correctamente.",
            "image": response.get("Attributes", {}),
        },
    )


def _get_image(table, image_id):
    response = table.get_item(Key={"image_id": image_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Imagen no encontrada.", "image_id": image_id},
        )

    return _build_response(
        200,
        {
            "message": "Imagen obtenida correctamente.",
            "image": item,
        },
    )


def _list_images(table):
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
            "message": "Imagenes obtenidas correctamente.",
            "count": len(items),
            "images": items,
        },
    )


def lambda_handler(event, context):
    method = _get_http_method(event)
    image_id = _get_path_parameters(event).get("image_id")

    try:
        if method == "OPTIONS":
            return _build_response(200, {"message": "CORS preflight OK."})

        table = _get_table()

        if method == "GET":
            if image_id:
                return _get_image(table, image_id)
            return _list_images(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_image(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_image(table, image_id, body)

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
                    {"message": "Ya existe una imagen con ese image_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {"message": "Imagen no encontrada.", "image_id": image_id},
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
