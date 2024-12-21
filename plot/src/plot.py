import pandas as pd
import matplotlib.pyplot as plt
import time

while True:
    try:
        # Читаем данные из файла
        df = pd.read_csv("/usr/src/app/logs/metric_log.csv")

        # Строим гистограмму распределения абсолютных ошибок
        plt.figure()
        df["absolute_error"].hist(bins=20)
        plt.title("Absolute Error Distribution")
        plt.xlabel("Absolute Error")
        plt.ylabel("Frequency")

        # Сохраняем график в файл
        plt.savefig("/usr/src/app/logs/error_distribution.png")

        # Wait before the next iteration
        time.sleep(5)
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
