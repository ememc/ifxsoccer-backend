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
    "image_date",
    "image_enabled",
)

UPDATABLE_IMAGE_FIELDS = (
    "image_title",
    "image_url",
    "image_date",
    "image_enabled",
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
    table_name = os.environ.get("IMAGES_TABLE_NAME", "images")
    if not table_name:
        raise ValueError("Falta la variable de entorno IMAGES_TABLE_NAME.")
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
    missing_fields = [field for field in REQUIRED_IMAGE_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    if not isinstance(body.get("image_enabled"), bool):
        raise ValueError("El campo image_enabled debe ser booleano.")


def _create_image(table, body):
    _validate_required_fields(body)

    image = {
        "image_id": body["image_id"],
        "image_title": body["image_title"],
        "image_url": body["image_url"],
        "image_date": body["image_date"],
        "image_enabled": body["image_enabled"],
    }

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

    if "image_enabled" in update_fields and not isinstance(
        update_fields["image_enabled"], bool
    ):
        raise ValueError("El campo image_enabled debe ser booleano.")

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
    method = (event.get("httpMethod") or "GET").upper()
    image_id = (event.get("pathParameters") or {}).get("image_id")

    try:
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
