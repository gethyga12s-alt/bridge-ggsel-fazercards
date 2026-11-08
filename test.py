def obrabotat_zakaz(order_id):
    # Твоя уже готовая логика — оставляем как есть
    kod = "TEST-CODE-" + order_id
    return kod

def test_unique_codes():
    # Получаем коды для двух разных заказов
    code1 = obrabotat_zakaz("001")
    code2 = obrabotat_zakaz("002")

    # Проверяем, что коды соответствуют ожиданиям
    assert code1 == "TEST-CODE-001", f"Ошибка: для заказа 001 получен код {code1}, а ожидалось TEST-CODE-001"
    assert code2 == "TEST-CODE-002", f"Ошибка: для заказа 002 получен код {code2}, а ожидалось TEST-CODE-002"

    # Проверяем, что коды действительно уникальны
    assert code1 != code2, "Ошибка: коды для разных заказов совпали!"

    print("✅ Все тесты пройдены!")

# Блок для запуска теста вручную (если нужно запустить просто как скрипт)
if __name__ == "__main__":
    test_unique_codes()

