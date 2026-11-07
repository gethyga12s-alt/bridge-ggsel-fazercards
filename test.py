import time

issued_orders = {}  # память о выданных заказах

def zapisat_v_log(soobshenie):
    print(time.strftime("%H:%M:%S"), "|", soobshenie)

def obrabotat_zakaz(order_id):
    # Шаг 1: проверка на дубль
    if order_id in issued_orders:
        zapisat_v_log("Заказ " + order_id + " пропущен: уже выдан ранее")
        return "пропущено"

    # Шаг 2: заглушка вместо покупки на Fazercards
    kod = "TEST-CODE-1234"

    # Шаг 3: заглушка вместо выдачи на Ggsel
    zapisat_v_log("Заказ " + order_id + " выдан, код скрыт")

    # Шаг 4: сразу запоминаем, что выдали
    issued_orders[order_id] = "issued"

    return "успех"

# Проверка: пробуем выдать один и тот же заказ дважды
print(obrabotat_zakaz("001"))
print(obrabotat_zakaz("001"))
