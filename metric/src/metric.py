import pika
import json
import pandas as pd


try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    # Объявляем очередь y_true
    channel.queue_declare(queue="y_true")
    # Объявляем очередь y_pred
    channel.queue_declare(queue="y_pred")

    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        # Прочитать данные из очереди
        message = json.loads(body)

        # Считаем csv файл metric_log.csv
        df = pd.read_csv("logs/metric_log.csv")
        if df.empty:
            df = pd.DataFrame(columns=["id", "y_true", "y_pred", "absolute_error"])
        id = message["id"]
        if method.routing_key == "y_pred":
            df.loc[df["id"] == id, "y_pred"] = message["y_pred"]
            # Считаем метрику
            metric = abs(
                df.loc[df["id"] == id, "y_true"] - df.loc[df["id"] == id, "y_pred"]
            )
            df.loc[df["id"] == id, "absolute_error"] = metric

            # Сохраняем результат в файл
            df.to_csv("logs/metric_log.csv", index=False)
            # Выводим результат
            print(f"Метрика для id {id} равна {metric.values[0]}")
        elif method.routing_key == "y_true":
            new_row = pd.DataFrame(
                [
                    {
                        "id": id,
                        "y_true": message["y_true"],
                        "y_pred": None,
                        "absolute_error": None,
                    }
                ]
            )
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv("logs/metric_log.csv", index=False)
            print(f"Получено значение y_true для id {id}")

        print(f"Из очереди {method.routing_key} получено значение {json.loads(body)}")

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(queue="y_true", on_message_callback=callback, auto_ack=True)
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(queue="y_pred", on_message_callback=callback, auto_ack=True)

    # Запускаем режим ожидания прихода сообщений
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()
except Exception as e:
    print("Не удалось подключиться к очереди, ошибка: ", e)
