import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_NEWS_FIELDS = (
    "news_id",
    "news_image",
    "news_date",
    "news_state",
    "news_enabled",
    "news_category",
    "news_program_by",
    "news_tags",
    "news_text",
)

UPDATABLE_NEWS_FIELDS = (
    "news_image",
    "news_date",
    "news_state",
    "news_enabled",
    "news_category",
    "news_program_by",
    "news_tags",
    "news_text",
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
    table_name = os.environ.get("NEWS_TABLE_NAME", "news")
    if not table_name:
        raise ValueError("Falta la variable de entorno NEWS_TABLE_NAME.")
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
    missing_fields = [field for field in REQUIRED_NEWS_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_news_field_types(body)


def _validate_news_field_types(fields):
    if "news_enabled" in fields and not isinstance(fields["news_enabled"], bool):
        raise ValueError("El campo news_enabled debe ser booleano.")


def _create_news(table, body):
    _validate_required_fields(body)

    news = {
        "news_id": body["news_id"],
        "news_image": body["news_image"],
        "news_date": body["news_date"],
        "news_state": body["news_state"],
        "news_enabled": body["news_enabled"],
        "news_category": body["news_category"],
        "news_program_by": body["news_program_by"],
        "news_tags": body["news_tags"],
        "news_text": body["news_text"],
    }

    table.put_item(
        Item=news,
        ConditionExpression="attribute_not_exists(news_id)",
    )

    return _build_response(
        201,
        {
            "message": "Noticia creada correctamente.",
            "news": news,
        },
    )


def _update_news(table, news_id, body):
    if not news_id:
        raise ValueError("Debes enviar news_id en la URL.")

    update_fields = {
        key: body[key] for key in UPDATABLE_NEWS_FIELDS if key in body
    }

    if not update_fields:
        raise ValueError(
            "Debes enviar al menos un campo para actualizar."
        )

    _validate_news_field_types(update_fields)

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
        Key={"news_id": news_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(news_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Noticia actualizada correctamente.",
            "news": response.get("Attributes", {}),
        },
    )


def _get_news(table, news_id):
    response = table.get_item(Key={"news_id": news_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Noticia no encontrada.", "news_id": news_id},
        )

    return _build_response(
        200,
        {
            "message": "Noticia obtenida correctamente.",
            "news": item,
        },
    )


def _list_news(table):
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
            "message": "Noticias obtenidas correctamente.",
            "count": len(items),
            "news": items,
        },
    )


def lambda_handler(event, context):
    method = (event.get("httpMethod") or "GET").upper()
    news_id = (event.get("pathParameters") or {}).get("news_id")

    try:
        table = _get_table()

        if method == "GET":
            if news_id:
                return _get_news(table, news_id)
            return _list_news(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_news(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_news(table, news_id, body)

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
                    {"message": "Ya existe una noticia con ese news_id."},
                )

            if method == "PUT":
                return _build_response(
                    404,
                    {"message": "Noticia no encontrada.", "news_id": news_id},
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