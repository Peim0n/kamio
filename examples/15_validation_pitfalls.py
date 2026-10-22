"""
15 — Validation Pitfalls (подводные камни валидации)
====================================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

В этом файле демонстрируются неочевидные поведения системы валидации Kamio:

    1. Конструктор kwargs ПОЛНОСТЬЮ обходят валидацию (object.__setattr__)
    2. _apply_defaults тоже обходят валидацию
    3. Строка "42" коэрцируется в float и сохраняется как float; "abc" → ValueError
    4. Choices: строка "42" != int 42 — валидация choices использует точное сравнение
    5. bool исключён из числового приведения (True на float-поле → ValueError)
    6. handle_state валидирует ВСЕ поля ДО применения ЛЮБОГО (атомарность)
    7. handle_config валидирует поле за полем (НЕ атомарно)
    8. Неизвестные/non-writable поля в handle_state тихо игнорируются (DEBUG log)
    9. required=True НЕ проверяется во время выполнения (только документация схемы)
   10. Опечатки в kwargs попадают в **metadata тихо (напр. writablee=True)
   11. _set_state обходит валидацию полностью (внутренний метод)
   12. Команды с префиксом set_ авто-маршрутизируются в handle_state (HA совместимость)

Запуск (БЕЗ MQTT-брокера)::

    python examples/15_validation_pitfalls.py
"""
from __future__ import annotations

import asyncio
import logging

from kamio import Device, command, config, state, telemetry
from kamio.data_fields import Field

# Тихое логирование, чтобы вывод был чистым
logging.basicConfig(level=logging.CRITICAL)


# =====================================================================
# 1. Конструктор kwargs обходят валидацию
# =====================================================================

class Thermostat(Device):
    """Термостат с min/max ограничениями."""
    temperature: float = state(default=22.0, min=0.0, max=100.0)
    mode: str = state(default="auto", choices=("auto", "manual", "off"))


def test_constructor_kwargs_bypass_validation():
    """kwargs в __init__ используют object.__setattr__ — валидация НЕ вызывается.

    НЕПРАВИЛЬНО (если ожидается валидация):
        dev = Thermostat(temperature=999.0)  # Ожидаем ValueError, но его нет!

    ПРАВИЛЬНО:
        dev = Thermostat()
        await dev.handle_state({"temperature": 999.0})  # ValueError здесь
    """
    # НЕПРАВИЛЬНО: 999.0 > max=100.0, но ошибки нет — kwargs обходят валидацию
    dev = Thermostat(temperature=999.0)
    assert dev.temperature == 999.0, "kwargs обошли валидацию: 999.0 принято без ошибки"

    # То же самое для choices: "invalid" нет в choices, но kwargs это пропускают
    dev2 = Thermostat(mode="invalid")
    assert dev2.mode == "invalid", "kwargs обошли choices: 'invalid' принято без ошибки"

    # ПРАВИЛЬНО: handle_state валидирует и вызывает ValueError
    dev3 = Thermostat()

    async def _check():
        try:
            await dev3.handle_state({"temperature": 999.0})
            assert False, "Должно было вызвать ValueError для 999.0 > max=100.0"
        except ValueError as e:
            assert "too high" in str(e), f"Ожидали 'too high', получили: {e}"

    asyncio.run(_check())
    print("[OK] 1. Конструктор kwargs обходят валидацию (object.__setattr__)")


# =====================================================================
# 2. _apply_defaults тоже обходят валидацию
# =====================================================================

class SensorWithBadDefault(Device):
    """Датчик с 'плохим' дефолтом, который нарушает min/max."""
    # default=-10 нарушает min=0, но _apply_defaults не валидирует
    pressure: float = state(default=-10.0, min=0.0, max=100.0)


def test_apply_defaults_bypass_validation():
    """_apply_defaults использует object.__setattr__ для всех полей.

    ВНИМАНИЕ: Дефолтные значения НЕ валидируются. Если default нарушает
    min/max, это не будет обнаружено до первой попытки установить значение
    через handle_state или __setattr__.
    """
    dev = SensorWithBadDefault()
    # Дефолт -10.0 нарушает min=0.0, но это не вызывает ошибку
    assert dev.pressure == -10.0, "Дефолт -10.0 установлен без валидации min=0.0"

    # Но прямое присваивание через __setattr__ валидирует!
    try:
        dev.pressure = -20.0
        assert False, "Должно было вызвать ValueError для -20.0 < min=0.0"
    except ValueError as e:
        assert "too low" in str(e)

    print("[OK] 2. _apply_defaults обходит валидацию (default=-10.0 при min=0.0)")


# =====================================================================
# 3. Строка "42" конвертируется для min/max, но "abc" пропускает min/max
# =====================================================================

class NumericField(Device):
    """Поле с числовыми ограничениями."""
    value: float = state(default=50.0, min=0.0, max=100.0)


def test_string_coercion_min_max():
    """_validate_value коэрцирует значение к объявленному типу поля.

    Строка "42" приводится к 42.0 и СОХРАНЯЕТСЯ как float — runtime-значение
    соответствует аннотации ``float``. Строка, которую нельзя привести
    к числу ("abc"), вызывает ValueError, а не тихо проходит.

    НЕПРАВИЛЬНО (если ожидается, что строка сохранится как строка):
        dev.value = "42"
        # Ожидаем dev.value == "42" — но будет 42.0 (float)!

    ПРАВИЛЬНО:
        Понимать, что _validate_value делает type coercion:
        значение нормализуется к типу аннотации или вызывает ValueError.
    """
    dev = NumericField()

    # "42" коэрцируется к 42.0 — сохраняется как float, не строка
    dev.value = "42"
    assert dev.value == 42.0 and isinstance(dev.value, float), (
        f"'42' коэрцировано в float: {dev.value!r}"
    )

    # "150" коэрцируется к 150.0 — max=100.0 нарушен → ValueError
    try:
        dev.value = "150"
        assert False, "Должно было вызвать ValueError: '150' → 150.0 > max=100.0"
    except ValueError as e:
        assert "too high" in str(e)

    # "abc" НЕ приводится к float → ValueError, значение не меняется
    try:
        dev.value = "abc"
        assert False, "Должно было вызвать ValueError: 'abc' не float"
    except ValueError as e:
        assert "Invalid type" in str(e)
    assert dev.value == 42.0, "Значение не изменилось после ошибки валидации"

    print('[OK] 3. "42" → 42.0 (float); "abc" → ValueError (type coercion)')


# =====================================================================
# 4. Choices: "42" (строка) != 42 (int) в choices
# =====================================================================

class ChoicesField(Device):
    """Поле с choices, определёнными как int."""
    level: int = state(default=1, choices=(1, 2, 3))


def test_choices_type_sensitivity():
    """choices использует точное сравнение (in frozenset).

    Строка "42" НЕ равна int 42 в frozenset({1, 2, 3}).
    Даже если значение числовое по смыслу, тип должен совпадать.

    НЕПРАВИЛЬНО:
        dev.level = "2"  # Ожидаем, что "2" == 2, но это не так!

    ПРАВИЛЬНО:
        dev.level = 2  # int 2 есть в choices (1, 2, 3)
    """
    dev = ChoicesField()

    # int 2 есть в choices — проходит
    dev.level = 2
    assert dev.level == 2

    # Строка "2" НЕ равна int 2 в frozenset — ValueError
    try:
        dev.level = "2"
        assert False, "Должно было вызвать ValueError: '2' (str) нет в choices (1, 2, 3) как int"
    except ValueError as e:
        assert "Must be one of" in str(e)

    print('[OK] 4. choices: "2" (str) != 2 (int) — точное сравнение типов')


# =====================================================================
# 5. bool исключён из числового приведения
# =====================================================================

class BoolExclusion(Device):
    """Поле с min/max, куда bool не должен попадать как 0.0/1.0."""
    flag: float = state(default=0.5, min=0.0, max=1.0)


def test_bool_excluded_from_numeric_coercion():
    """bool-значение отклоняется для не-bool поля (никакой коэрции True → 1.0).

    isinstance(True, int) == True в Python, и float(True) == 1.0 — поэтому
    _validate_value явно отклоняет bool для полей с не-bool типом:
    никакой неявной конвертации True в 1.0 или "True".

    НЕПРАВИЛЬНО (если ожидается, что True примется как 1.0):
        dev.flag = True  # ValueError, а не 1.0!

    ПРАВИЛЬНО:
        Понимать, что bool отклоняется для не-bool полей, а bool-поле
        принимает только реальные bool (не 0/1, не "true").
    """
    dev = BoolExclusion()

    # True на float-поле → ValueError (bool не коэрцируется в 1.0)
    try:
        dev.flag = True
        assert False, "Должно было вызвать ValueError: bool на float-поле"
    except ValueError as e:
        assert "Invalid type" in str(e)

    # False тоже отклоняется
    try:
        dev.flag = False
        assert False, "Должно было вызвать ValueError: bool на float-поле"
    except ValueError as e:
        assert "Invalid type" in str(e)

    # Для сравнения: int 2 коэрцируется в 2.0 и нарушает max=1.0
    try:
        dev.flag = 2
        assert False, "Должно было вызвать ValueError: 2.0 > max=1.0"
    except ValueError as e:
        assert "too high" in str(e)

    # И симметрично: bool-поле принимает только bool
    class BoolField(Device):
        flag: bool = state(default=False)

    bf = BoolField()
    bf.flag = True
    assert bf.flag is True
    try:
        bf.flag = 1  # int 1 — не bool → ValueError
        assert False, "Должно было вызвать ValueError: int на bool-поле"
    except ValueError as e:
        assert "expected bool" in str(e)

    print("[OK] 5. bool исключён из приведения (True на float → ValueError)")


# =====================================================================
# 6. handle_state валидирует ВСЕ поля ДО применения ЛЮБОГО (атомарность)
# =====================================================================

class AtomicValidation(Device):
    """Устройство с двумя полями для проверки атомарности."""
    a: int = state(default=10, min=0, max=100)
    b: int = state(default=20, min=0, max=100)


def test_handle_state_atomic_validation():
    """handle_state сначала валидирует ВСЕ поля, потом применяет.

    Если одно поле невалидно, НИ ОДНО поле не применяется.
    Это атомарность: all-or-nothing.

    НЕПРАВИЛЬНО (если ожидается частичное применение):
        await dev.handle_state({"a": 50, "b": 999})
        # Ожидаем a=50, b=старое — но на самом деле a тоже не меняется!

    ПРАВИЛЬНО:
        Понимать, что handle_state атомарен: если b невалидно, a не меняется.
    """
    dev = AtomicValidation()
    assert dev.a == 10 and dev.b == 20

    async def _check():
        # b=999 невалидно (max=100) — вся операция отклоняется
        try:
            result = await dev.handle_state({"a": 50, "b": 999})
            assert False, "Должно было вызвать ValueError для b=999"
        except ValueError:
            pass

        # a НЕ изменилось, хотя оно было валидным — атомарность!
        assert dev.a == 10, f"a должно остаться 10 (атомарность), но стало {dev.a}"
        assert dev.b == 20, f"b должно остаться 20, но стало {dev.b}"

        # Теперь валидный запрос — оба применяются
        result = await dev.handle_state({"a": 50, "b": 60})
        assert result == {"a": 50, "b": 60}
        assert dev.a == 50 and dev.b == 60

    asyncio.run(_check())
    print("[OK] 6. handle_state атомарен: если b невалидно, a тоже не применяется")


# =====================================================================
# 7. handle_config валидирует поле за полем (НЕ атомарно)
# =====================================================================

class NonAtomicConfig(Device):
    """Устройство с config-полями."""
    threshold: float = config(default=22.0)
    offset: float = config(default=0.0)


def test_handle_config_non_atomic():
    """handle_config применяет поля по одному, валидируя каждое.

    Если второе поле невалидно, первое УЖЕ применено — НЕ атомарно!

    НЕПРАВИЛЬНО (если ожидается атомарность config):
        await dev.handle_config({"threshold": 30.0, "offset": "bad"})
        # Ожидаем, что threshold не изменился — но он уже изменился!

    ПРАВИЛЬНО:
        Понимать, что handle_config не атомарен.
        Валидируйте данные ДО вызова, если нужна атомарность.
    """
    dev = NonAtomicConfig()
    assert dev.threshold == 22.0 and dev.offset == 0.0

    async def _check():
        # threshold=30.0 валидно и применяется, потом offset="bad" вызывает ValueError
        # НО threshold уже изменён!
        try:
            await dev.handle_config({"threshold": 30.0, "offset": "bad_value"})
            assert False, "Должно было вызвать ValueError для offset='bad_value'"
        except ValueError:
            pass

        # threshold УЖЕ изменился, хотя offset не прошёл — НЕ атомарно!
        assert dev.threshold == 30.0, (
            f"threshold должно быть 30.0 (уже применено до ошибки), но {dev.threshold}"
        )
        # offset не изменился, т.к. ошибка произошла до его применения
        assert dev.offset == 0.0

    asyncio.run(_check())
    print("[OK] 7. handle_config НЕ атомарен: threshold=30.0 применён до ошибки offset")


# =====================================================================
# 8. Unknown/non-writable поля в handle_state тихо игнорируются
# =====================================================================

class WritableFields(Device):
    """Устройство с writable и non-writable полями."""
    power: bool = state(default=False, writable=True)
    firmware_version: str = state(default="1.0", writable=False)
    energy: float = telemetry(default=0.0, unit="Wh", freq="10s")


def test_unknown_and_non_writable_ignored():
    """handle_state тихо игнорирует неизвестные и non-writable поля.

    Неизвестные поля → DEBUG log, пропуск.
    Non-writable state поля → DEBUG log, пропуск.
    Telemetry поля → DEBUG log, пропуск (kind != "state").

    НЕПРАВИЛЬНО (если ожидается ошибка для неизвестного поля):
        await dev.handle_state({"unknown_field": 123})
        # Ожидаем KeyError/ValueError — но тихо игнорируется!

    ПРАВИЛЬНО:
        Понимать, что handle_state возвращает только применённые изменения.
        Проверяйте результат, чтобы узнать, что было применено.
    """
    dev = WritableFields()

    async def _check():
        # Неизвестное поле — тихо игнорируется
        result = await dev.handle_state({"unknown_field": 123})
        assert result == {}, "Неизвестное поле тихо проигнорировано, результат пуст"

        # Non-writable state поле — тихо игнорируется
        result = await dev.handle_state({"firmware_version": "2.0"})
        assert result == {}, "Non-writable поле тихо проигнорировано"
        assert dev.firmware_version == "1.0", "firmware_version не изменился"

        # Telemetry поле — тихо игнорируется (kind != "state")
        result = await dev.handle_state({"energy": 99.0})
        assert result == {}, "Telemetry поле тихо проигнорировано в handle_state"

        # Writable state поле — применяется
        result = await dev.handle_state({"power": True})
        assert result == {"power": True}, "power применён"
        assert dev.power is True

    asyncio.run(_check())
    print("[OK] 8. Unknown/non-writable/telemetry поля тихо игнорируются в handle_state")


# =====================================================================
# 9. required=True НЕ проверяется во время выполнения
# =====================================================================

class RequiredField(Device):
    """Поле с required=True — но это только документация."""
    sensor_id: str = state(default=None, required=True)
    temperature: float = telemetry(default=0.0, required=True)


def test_required_not_enforced():
    """required=True — это только метка для схемы, НЕ валидация.

    Ни __init__, ни handle_state, ни _validate_value не проверяют required.
    Поле со значением None принимается без ошибок.

    НЕПРАВИЛЬНО (если ожидается ошибка для None при required=True):
        dev = RequiredField()  # sensor_id=None, но required=True
        # Ожидаем ошибку — но её нет!

    ПРАВИЛЬНО:
        Проверяйте required вручную в on_init или внешним кодом.
    """
    dev = RequiredField()
    # sensor_id=None несмотря на required=True — никакой ошибки
    assert dev.sensor_id is None, "required=True не предотвращает None"
    assert dev.temperature == 0.0

    # Проверим, что required=True есть в схеме (это его единственное применение)
    schema = RequiredField.get_schema()
    assert schema["fields"]["sensor_id"]["required"] is True, "required=True в схеме"

    # handle_state тоже не проверяет required: None проходит валидацию.
    # Поскольку старое значение тоже None, applied_changes пуст
    # (handle_state возвращает только фактические изменения).
    async def _check():
        result = await dev.handle_state({"sensor_id": None})
        assert result == {}, "None→None не считается изменением"
        # А теперь реальное изменение на str — применяется
        result = await dev.handle_state({"sensor_id": "s-1"})
        assert result == {"sensor_id": "s-1"}, "str применён при required=True"

    asyncio.run(_check())
    print("[OK] 9. required=True НЕ проверяется в runtime — только в схеме")


# =====================================================================
# 10. Опечатки в kwargs попадают в **metadata тихо
# =====================================================================

class TypoField(Device):
    """Демонстрация опечатки в параметре state()."""
    # Опечатка: writablee вместо writable
    # writable=True (правильный параметр) не передан → default=True
    # writablee=True попадает в **metadata тихо
    power: bool = state(default=False, writablee=True)


def test_misspelled_kwargs_into_metadata():
    """Опечатки в параметрах state()/telemetry() попадают в **metadata.

    state(writablee=True) — 'writablee' не является параметром state(),
    поэтому попадает в **metadata. Поле остаётся writable=True (дефолт).

    НЕПРАВИЛЬНО:
        power: bool = state(default=False, writablee=True)
        # Думаем, что сделали поле writable, но 'writablee' — опечатка

    ПРАВИЛЬНО:
        power: bool = state(default=False, writable=False)
        # Правильный параметр 'writable'
    """
    dev = TypoField()

    field = TypoField.Kamio_FIELDS["power"]
    # writable остался True (дефолт), потому что 'writablee' не распознан
    assert field.writable is True, (
        "writable=True (дефолт), т.к. 'writablee' попал в metadata, а не в writable"
    )
    # Опечатка 'writablee' тихо лежит в metadata
    assert "writablee" in field.metadata, "Опечатка 'writablee' попала в metadata"
    assert field.metadata["writablee"] is True

    print("[OK] 10. Опечатка 'writablee=True' попала в metadata, writable остался True")


# =====================================================================
# 11. _set_state обходит валидацию полностью
# =====================================================================

class SetStateBypass(Device):
    """Устройство для демонстрации _set_state."""
    temperature: float = state(default=22.0, min=0.0, max=100.0)
    mode: str = state(default="auto", choices=("auto", "manual"))


def test_set_state_bypasses_validation():
    """_set_state использует object.__setattr__ — без валидации и публикации.

    Это внутренний метод для зеркалирования состояния без re-publish.
    Он НЕ вызывает _validate_value и НЕ публикует в MQTT.

    НЕПРАВИЛЬНО (если ожидается валидация):
        dev._set_state(temperature=999.0)  # Ожидаем ValueError — но нет!

    ПРАВИЛЬНО:
        Использовать handle_state для валидируемых изменений.
        _set_state — только для внутреннего зеркалирования.
    """
    dev = SetStateBypass()

    # _set_state принимает 999.0 без проверки min/max
    dev._set_state(temperature=999.0)
    assert dev.temperature == 999.0, "_set_state обошёл min/max: 999.0 принято"

    # _set_state принимает "invalid" без проверки choices
    dev._set_state(mode="invalid")
    assert dev.mode == "invalid", "_set_state обошёл choices: 'invalid' принято"

    # Для сравнения: __setattr__ валидирует
    try:
        dev.temperature = 999.0
        assert False, "__setattr__ должен вызвать ValueError для 999.0 > max=100.0"
    except ValueError:
        pass

    print("[OK] 11. _set_state обходит валидацию (object.__setattr__ без _validate_value)")


# =====================================================================
# 12. set_ prefix команды авто-маршрутизируются в handle_state
# =====================================================================

class HACompatible(Device):
    """Устройство для HA-совместимости через set_ prefix."""
    brightness: int = state(default=100, min=0, max=255, writable=True)
    read_only: str = state(default="firmware", writable=False)

    @command
    async def set_custom(self, value: int):
        """Команда с set_ prefix, но определённая явно — не авто-маршрутизируется."""
        return {"custom": value}


def test_set_prefix_auto_routing():
    """Команды set_<field> авто-маршрутизируются в handle_state.

    Если нет явно определённой команды с именем set_<field>,
    handle_command проверяет: если method_name начинается с "set_"
    и field_name есть в Kamio_FIELDS и это writable state —
    вызывает handle_state({field_name: value}).

    НЕПРАВИЛЬНО (если ожидается AttributeError для set_brightness):
        await dev.handle_command("set_brightness", {"value": 200})
        # Ожидаем "Command not found" — но авто-маршрутизация срабатывает!

    ПРАВИЛЬНО:
        Понимать, что set_<writable_state_field> работает автоматически.
    """
    dev = HACompatible()

    async def _check():
        # set_brightness нет в Kamio_COMMANDS, но авто-маршрутизация работает
        result = await dev.handle_command("set_brightness", {"value": 200})
        assert result == {"brightness": 200}, f"Авто-маршрутизация set_brightness: {result}"
        assert dev.brightness == 200

        # set_read_only: поле non-writable → AttributeError (не маршрутизируется)
        try:
            await dev.handle_command("set_read_only", {"value": "new"})
            assert False, "Должно вызвать AttributeError: read_only non-writable"
        except AttributeError:
            pass

        # set_custom: есть в Kamio_COMMANDS → вызывается как команда, НЕ маршрутируется
        result = await dev.handle_command("set_custom", {"value": 42})
        assert result == {"custom": 42}, "set_custom вызван как команда (не handle_state)"

    asyncio.run(_check())
    print("[OK] 12. set_brightness → handle_state; set_custom → команда; set_read_only → AttributeError")


# =====================================================================
# Главная функция
# =====================================================================

def main():
    print("=" * 70)
    print("15 — Validation Pitfalls: подводные камни валидации Kamio")
    print("=" * 70)
    print()

    test_constructor_kwargs_bypass_validation()
    test_apply_defaults_bypass_validation()
    test_string_coercion_min_max()
    test_choices_type_sensitivity()
    test_bool_excluded_from_numeric_coercion()
    test_handle_state_atomic_validation()
    test_handle_config_non_atomic()
    test_unknown_and_non_writable_ignored()
    test_required_not_enforced()
    test_misspelled_kwargs_into_metadata()
    test_set_state_bypasses_validation()
    test_set_prefix_auto_routing()

    print()
    print("=" * 70)
    print("Все тесты прошли! Все поведения подтверждены.")
    print("=" * 70)


if __name__ == "__main__":
    main()
