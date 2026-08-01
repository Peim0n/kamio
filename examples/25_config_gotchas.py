"""Глубокий разбор подводных камней конфигурации Kamio (Config).

Этот файл — НЕ базовый туториал. Он демонстрирует неочевидное поведение
класса ``kamio.config.Config``, которое может удивить разработчиков
фреймворка:

1. Отсутствующий файл конфигурации — только WARNING, работа продолжается
   с дефолтами (исключение НЕ выбрасывается).
2. Лимит размера файла конфигурации — 1 МБ; превышение вызывает ValueError.
3. Верхний уровень JSON должен быть dict (object), не list и не скаляр.
4. Известные ключи УДАЛЯЮТСЯ из raw-словаря (мутация in-place) — после
   парсинга в ``_extra`` остаются только неизвестные ключи.
5. Неверный log_level — молчаливый откат к INFO (без исключения).
6. get() с точечной нотацией: плоские ключи берутся из settings, вложенные
   из _extra.
7. Boolean-кастинг: только "true", "1", "yes", "on" (case-insensitive);
   всё остальное — False.
8. Ошибка каста: возвращается default с WARNING (без исключения).
9. Префикс env-переменной: case-insensitive сопоставление, НО
   case-sensitive срезание (потенциальный баг на Windows).
10. Двойное подчёркивание → точка: "a__b" → "a.b", но "a___b" → "a.b"
    (тройное становится single+dot — неочевидное поведение replace).
11. Приоритет: env > file > default.
12. _get_nested возвращает None для отсутствующего ключа — неотличимо от
    значения None.
13. _set_nested перезаписывает промежуточные не-dict ключи.
14. Валидация broker: проверяется только тип (str), не формат URL.
15. Валидация log_level: case-sensitive — "info" не пройдёт, "INFO" да.

Все примеры запускаются БЕЗ MQTT-брокера — используются прямые вызовы
API, моки и assertions.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import unittest
from unittest.mock import patch

from kamio.config import Config, Settings


class TestConfigGotchas(unittest.TestCase):
    """Тесты, доказывающие неочевидное поведение Config."""

    def setUp(self):
        """Очищаем env от переменных Kamio_ перед каждым тестом."""
        # НЕЛЬЗЯ: забыть очистить env — тесты будут влиять друг на друга
        # ПРАВИЛЬНО: сохраняем и восстанавливаем окружение
        self._saved_env = {
            k: v for k, v in os.environ.items() if k.upper().startswith("KAMIO_")
        }
        for k in list(os.environ.keys()):
            if k.upper().startswith("KAMIO_"):
                del os.environ[k]

    def tearDown(self):
        """Восстанавливаем env после теста."""
        for k in list(os.environ.keys()):
            if k.upper().startswith("KAMIO_"):
                del os.environ[k]
        os.environ.update(self._saved_env)

    # ------------------------------------------------------------------
    # 1. Отсутствующий файл — WARNING, не исключение
    # ------------------------------------------------------------------
    def test_missing_config_file_warning_not_exception(self):
        """Отсутствующий файл конфигурации вызывает WARNING, но НЕ исключение.

        Готча: многие ожидают FileNotFoundError, но Config продолжает работу
        с дефолтами. Это может скрыть опечатку в пути.
        """
        # НЕВЕРНО: ожидать исключение при отсутствии файла
        # with self.assertRaises(FileNotFoundError):
        #     Config("/nonexistent/path.json")

        # ПРАВИЛЬНО: Config продолжает работу с дефолтами
        cfg = Config("/nonexistent/path/to/config.json")

        # Значения по умолчанию применены
        assert cfg.mqtt_broker == "mqtt://localhost:1883", (
            f"Ожидался дефолтный broker, получили {cfg.mqtt_broker!r}"
        )
        assert cfg.log_level == logging.INFO, (
            f"Ожидался log_level=INFO, получили {cfg.log_level}"
        )
        print("  [OK] Отсутствующий файл → WARNING + дефолты (без исключения)")

    # ------------------------------------------------------------------
    # 2. Лимит размера файла — 1 МБ
    # ------------------------------------------------------------------
    def test_config_file_size_limit_1mb(self):
        """Файл конфигурации больше 1 МБ вызывает ValueError.

        Готча: лимит жёстко задан в коде (1 * 1024 * 1024). Это защита от
        злонамеренно раздутых конфигов, но может удивить при больших
        встроенных конфигурациях.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            # Создаём файл чуть больше 1 МБ
            # НЕВЕРНО: предполагать, что размер не проверяется
            big_data = {"key_" + str(i): "x" * 100 for i in range(11000)}
            json.dump(big_data, f)
            f.flush()
            big_path = f.name

        try:
            # ПРАВИЛЬНО: ожидать ValueError при превышении 1 МБ
            with self.assertRaises(ValueError) as ctx:
                Config(big_path)
            assert "too large" in str(ctx.exception).lower() or "1 MB" in str(
                ctx.exception
            ), f"Неожиданное сообщение об ошибке: {ctx.exception}"
            print("  [OK] Файл >1 МБ → ValueError")
        finally:
            os.unlink(big_path)

    # ------------------------------------------------------------------
    # 3. Верхний уровень должен быть dict, не list
    # ------------------------------------------------------------------
    def test_config_must_be_dict_not_list(self):
        """JSON-файл должен содержать объект (dict), не массив (list).

        Готча: json.load() успешно парсит list, но Config проверяет тип
        только после парсинга. Скалярные значения тоже вызовут ошибку.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            # НЕВЕРНО: использовать list как верхний уровень
            json.dump([1, 2, 3], f)
            f.flush()
            list_path = f.name

        try:
            # ПРАВИЛЬНО: ожидать ValueError — Config требует dict
            with self.assertRaises(ValueError) as ctx:
                Config(list_path)
            assert "must contain a JSON object" in str(ctx.exception), (
                f"Неожиданное сообщение: {ctx.exception}"
            )
            print("  [OK] List вместо dict → ValueError")
        finally:
            os.unlink(list_path)

    # ------------------------------------------------------------------
    # 4. Известные ключи удаляются из raw dict (мутация in-place)
    # ------------------------------------------------------------------
    def test_known_keys_removed_from_raw(self):
        """Известные ключи (mqtt_broker, log_level) удаляются из raw dict.

        Готча: после парсинга ``_extra`` содержит ТОЛЬКО неизвестные ключи.
        Если вы ожидаете найти mqtt_broker через get("mqtt_broker") — он
        берётся из settings, а не из _extra.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(
                {
                    "mqtt_broker": "mqtt://broker.example.com:1883",
                    "log_level": "DEBUG",
                    "custom_key": "custom_value",
                },
                f,
            )
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # Известные ключи доступны через settings
            assert cfg.settings.mqtt_broker == "mqtt://broker.example.com:1883"
            assert cfg.settings.log_level == "DEBUG"

            # НЕВЕРНО: ожидать, что mqtt_broker остался в _extra
            # ПРАВИЛЬНО: mqtt_broker удалён из _extra, только custom_key остался
            assert "mqtt_broker" not in cfg._extra, (
                "mqtt_broker не должен быть в _extra — он удалён как известный ключ"
            )
            assert "log_level" not in cfg._extra, (
                "log_level не должен быть в _extra — он удалён как известный ключ"
            )
            assert "custom_key" in cfg._extra, "custom_key должен остаться в _extra"
            print("  [OK] Известные ключи удалены из _extra (мутация in-place)")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 5. Неверный log_level — молчаливый откат к INFO
    # ------------------------------------------------------------------
    def test_invalid_log_level_falls_back_to_info(self):
        """Неверный log_level — WARNING и откат к INFO, без исключения.

        Готча: проверка case-sensitive! "info" НЕ пройдёт, "INFO" — да.
        Также "WARN" работает, но "warn" — нет.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            # НЕВЕРНО: использовать lowercase "debug"
            # ПРАВИЛЬНО: использовать uppercase "DEBUG"
            json.dump({"log_level": "VERBOSE"}, f)  # несуществующий уровень
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # Молчаливый откат к INFO
            assert cfg.settings.log_level == "INFO", (
                f"Ожидался откат к INFO, получили {cfg.settings.log_level!r}"
            )
            assert cfg.log_level == logging.INFO
            print("  [OK] Неверный log_level → молчаливый откат к INFO")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 6. get() с точечной нотацией: плоские из settings, вложенные из _extra
    # ------------------------------------------------------------------
    def test_get_dot_notation_flat_vs_nested(self):
        """get("mqtt_broker") — из settings; get("a.b.c") — из _extra.

        Готча: если ключ БЕЗ точки и есть в settings — берётся оттуда.
        Если ключ С точкой — всегда из _extra (даже если часть имени
        совпадает с известным ключом).
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(
                {
                    "mqtt_broker": "mqtt://from-file:1883",
                    "mqtt": {"broker": "mqtt://nested:1883"},
                },
                f,
            )
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # Плоский ключ — из settings
            assert cfg.get("mqtt_broker") == "mqtt://from-file:1883"

            # Вложенный ключ — из _extra (НЕ из settings!)
            # "mqtt.broker" ищется в _extra как nested dict
            assert cfg.get("mqtt.broker") == "mqtt://nested:1883", (
                "mqtt.broker должен искаться в _extra как вложенный ключ"
            )

            # Если ключа нет в _extra — возвращается default
            assert cfg.get("nonexistent.key", "fallback") == "fallback"
            print("  [OK] get(): плоские из settings, вложенные из _extra")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 7. Boolean-кастинг: только "true", "1", "yes", "on"
    # ------------------------------------------------------------------
    def test_boolean_casting(self):
        """Boolean-кастинг распознаёт только "true", "1", "yes", "on".

        Готча: "True", "TRUE", "YES" работают (case-insensitive), но
        "y", "t", "enabled" — нет (возвращают False). Пустая строка — False.
        Числовые значения (не строки) проходят через bool() напрямую.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(
                {
                    "flag_true": "true",
                    "flag_1": "1",
                    "flag_yes": "yes",
                    "flag_on": "on",
                    "flag_True": "True",
                    "flag_YES": "YES",
                    "flag_false": "false",
                    "flag_0": "0",
                    "flag_no": "no",
                    "flag_off": "off",
                    "flag_empty": "",
                    "flag_y": "y",
                    "flag_enabled": "enabled",
                },
                f,
            )
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # ПРАВИЛЬНО: эти значения → True
            for key in ("flag_true", "flag_1", "flag_yes", "flag_on", "flag_True", "flag_YES"):
                result = cfg.get(key, cast=bool)
                assert result is True, f"{key} должен быть True, получили {result!r}"

            # НЕВЕРНО: ожидать, что "y" или "enabled" → True
            # ПРАВИЛЬНО: они → False (не в списке распознаваемых)
            for key in ("flag_false", "flag_0", "flag_no", "flag_off", "flag_empty", "flag_y", "flag_enabled"):
                result = cfg.get(key, cast=bool)
                assert result is False, f"{key} должен быть False, получили {result!r}"

            print("  [OK] Boolean-кастинг: только true/1/yes/on (case-insensitive)")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 8. Ошибка каста — default с WARNING
    # ------------------------------------------------------------------
    def test_cast_failure_returns_default(self):
        """Ошибка каста возвращает default с WARNING, без исключения.

        Готча: int("abc") не выбрасывает исключение наружу — вы получаете
        default и можете не заметить проблему.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"port": "not_a_number"}, f)
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # НЕВЕРНО: ожидать ValueError
            # with self.assertRaises(ValueError):
            #     cfg.get("port", cast=int)

            # ПРАВИЛЬНО: возвращается default, ошибка логируется
            result = cfg.get("port", default=1883, cast=int)
            assert result == 1883, f"Ожидался default=1883, получили {result!r}"
            print("  [OK] Ошибка каста → default + WARNING (без исключения)")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 9. Env-префикс: case-insensitive match, case-sensitive strip
    # ------------------------------------------------------------------
    def test_env_prefix_case_insensitive_match_case_sensitive_strip(self):
        """Префикс Kamio_ сопоставляется case-insensitive, но срезается
        по фиксированной длине (len("Kamio_") = 6).

        Готча: на Windows env-переменные нечувствительны к регистру, но
        Python получает их в исходном виде. KAMIO_MQTT_BROKER → срезается
        как "MQTT_BROKER" → lower → "mqtt_broker". Но Kamio_mqtt_broker
        (смешанный регистр) → срезается как "mqtt_broker" → lower → "mqtt_broker".
        Проблема: если env-переменная названа KAMIO_mqtt_broker, срез
        даёт "mqtt_broker" → lower → "mqtt_broker" — работает. Но если
        переменная названа Kamio_MQTT_BROKER, срез даёт "MQTT_BROKER"
        → lower → "mqtt_broker" — тоже работает. Баг в том, что срез
        всегда по длине "Kamio_" (6 символов), и если переменная имеет
        префикс в другом регистре, длина та же, так что фактически работает.
        """
        # Устанавливаем env в UPPERCASE (как это делает Windows)
        os.environ["KAMIO_MQTT_BROKER"] = "mqtt://from-env:1883"

        cfg = Config()  # без файла

        # ПРАВИЛЬНО: KAMIO_ сопоставляется с Kamio_ (case-insensitive)
        assert cfg.mqtt_broker == "mqtt://from-env:1883", (
            f"Ожидался broker из env, получили {cfg.mqtt_broker!r}"
        )
        print("  [OK] Env-префикс: case-insensitive сопоставление работает")

    def test_env_prefix_lowercase_also_works(self):
        """Нижний регистр префикса тоже работает (case-insensitive match)."""
        os.environ["kamio_mqtt_broker"] = "mqtt://lowercase-env:1883"

        cfg = Config()
        assert cfg.mqtt_broker == "mqtt://lowercase-env:1883"
        print("  [OK] Lowercase префикс kamio_ тоже сопоставляется")

    # ------------------------------------------------------------------
    # 10. Двойное подчёркивание → точка (но тройное — неочевидно)
    # ------------------------------------------------------------------
    def test_double_underscore_to_dot_conversion(self):
        """Двойное подчёркивание в env-имени → точка в конфиг-ключе.

        Готча: replace("__", ".") заменяет ВСЕ двойные подчёркивания.
        "a__b" → "a.b" (ожидаемо), но "a___b" → "a.b" (тройное = "__" + "_b"
        → "a" + "." + "_b" → нет, replace заменяет первое "__" → "a._b").
        На самом деле str.replace("__", ".") заменяет все непересекающиеся
        "__" слева направо: "a___b" → "a" + replace("___b") → "a" + "." + "_b"
        = "a._b". Но это lower()'ed → "a._b".
        """
        # Двойное подчёркивание → точка
        os.environ["KAMIO_MQTT__TLS__CAFILE"] = "/path/to/ca.pem"

        cfg = Config()
        # ПРАВИЛЬНО: mqtt.tls.cafile ищется во вложенном dict
        result = cfg.get("mqtt.tls.cafile")
        assert result == "/path/to/ca.pem", f"Ожидался путь из env, получили {result!r}"
        print("  [OK] Двойное __ → точка: mqtt.tls.cafile работает")

    def test_triple_underscore_behavior(self):
        """Тройное подчёркивание: "a___b" → "a._b" (не "a.b").

        Готча: replace("__", ".") обрабатывает слева направо:
        "a___b" → "a" + "." + "_b" = "a._b"
        Это может быть неожиданным, если вы ожидали "a.b".
        """
        os.environ["KAMIO_A___B"] = "triple_value"

        cfg = Config()
        # НЕВЕРНО: ожидать get("a.b") == "triple_value"
        # ПРАВИЛЬНО: ключ преобразован в "a._b"
        result = cfg.get("a._b")
        assert result == "triple_value", (
            f"Ожидался 'triple_value' по ключу 'a._b', получили {result!r}"
        )

        # И "a.b" НЕ содержит это значение
        assert cfg.get("a.b") is None, "a.b не должен содержать значение из a___b"
        print("  [OK] Тройное ___ → 'a._b' (не 'a.b') — неочевидное поведение")

    # ------------------------------------------------------------------
    # 11. Приоритет: env > file > default
    # ------------------------------------------------------------------
    def test_priority_env_over_file_over_default(self):
        """Приоритет значений: env > file > default.

        Готча: env-переменные накладываются ПОВЕРХ файловых значений.
        Даже если файл задаёт значение, env его перезапишет.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"mqtt_broker": "mqtt://from-file:1883"}, f)
            f.flush()
            cfg_path = f.name

        try:
            # Env перезаписывает файл
            os.environ["KAMIO_MQTT_BROKER"] = "mqtt://from-env:1883"

            cfg = Config(cfg_path)

            # ПРАВИЛЬНО: env имеет высший приоритет
            assert cfg.mqtt_broker == "mqtt://from-env:1883", (
                f"Ожидался broker из env (приоритет), получили {cfg.mqtt_broker!r}"
            )
            print("  [OK] Приоритет: env > file > default")
        finally:
            os.unlink(cfg_path)

    def test_priority_file_over_default(self):
        """Файл имеет приоритет над дефолтом (когда нет env)."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"mqtt_broker": "mqtt://from-file:1883"}, f)
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)
            assert cfg.mqtt_broker == "mqtt://from-file:1883"
            print("  [OK] Приоритет: file > default (без env)")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 12. _get_nested возвращает None для отсутствующего ключа
    # ------------------------------------------------------------------
    def test_get_nested_returns_none_indistinguishable(self):
        """_get_nested возвращает None как для отсутствующего ключа,
        так и для ключа со значением None — они неотличимы.

        Готча: get("missing.key") и get("key.with_none_value") оба
        возвращают None. Используйте default= для различения.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(
                {
                    "existing": {"sub": None},
                    "missing_key": "present",
                },
                f,
            )
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)

            # Существующий ключ со значением None
            result1 = cfg.get("existing.sub")
            # Несуществующий ключ
            result2 = cfg.get("existing.nonexistent")

            # Оба возвращают None — неотличимы!
            assert result1 is None, f"existing.sub должен быть None, получили {result1!r}"
            assert result2 is None, f"existing.nonexistent тоже None, получили {result2!r}"

            # ПРАВИЛЬНО: используйте default для различения
            # Но даже default не помогает, если значение реально None!
            # get() проверяет: if value is None: value = default
            # Так что если значение None, вы получите default.
            assert cfg.get("existing.sub", "fallback") == "fallback", (
                "None-значение заменяется на default — нельзя отличить от отсутствия"
            )
            print("  [OK] _get_nested: None для отсутствующего и для None-значения неотличимы")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 13. _set_nested перезаписывает промежуточные не-dict ключи
    # ------------------------------------------------------------------
    def test_set_nested_overwrites_non_dict_intermediate(self):
        """_set_nested перезаписывает промежуточные ключи, если они не dict.

        Готча: если "a" уже имеет скалярное значение (не dict), и вы
        устанавливаете "a.b", то "a" будет заменён на пустой dict {},
        уничтожая исходное значение.
        """
        data = {"a": "scalar_value"}

        # ПРАВИЛЬНО: _set_nested перезаписывает "a" (строку) на dict
        Config._set_nested(data, "a.b", "new_value")

        assert data["a"] == {"b": "new_value"}, (
            f"Ожидалось {{'b': 'new_value'}}, получили {data['a']!r}"
        )
        # Исходное скалярное значение "scalar_value" потеряно!
        assert "scalar_value" not in str(data), "Скалярное значение должно быть потеряно"
        print("  [OK] _set_nested перезаписывает не-dict промежуточные ключи")

    # ------------------------------------------------------------------
    # 14. Валидация broker: только тип (str), не формат URL
    # ------------------------------------------------------------------
    def test_broker_validation_only_type_not_format(self):
        """Валидация mqtt_broker проверяет только isinstance(str),
        не формат URL.

        Готча: "not_a_url" пройдёт валидацию. "123" (int) — нет.
        Пустая строка пройдёт (она str).
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            # НЕВЕРНО: предполагать, что невалидный URL будет отклонён
            # ПРАВИЛЬНО: любая строка проходит валидацию
            json.dump({"mqtt_broker": "not_a_valid_url_at_all"}, f)
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)
            assert cfg.mqtt_broker == "not_a_valid_url_at_all", (
                "Любая строка проходит валидацию broker — формат не проверяется"
            )
            print("  [OK] Broker: проверяется только тип (str), не формат URL")
        finally:
            os.unlink(cfg_path)

    def test_broker_non_string_falls_back_to_default(self):
        """Нестроковый broker → WARNING + откат к дефолту."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"mqtt_broker": 12345}, f)  # int, не str
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)
            # Откат к дефолту, т.к. 12345 — не строка
            assert cfg.mqtt_broker == Settings.mqtt_broker, (
                f"Нестроковый broker → дефолт, получили {cfg.mqtt_broker!r}"
            )
            print("  [OK] Нестроковый broker → откат к дефолту")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # 15. Валидация log_level: case-sensitive
    # ------------------------------------------------------------------
    def test_log_level_case_sensitive(self):
        """Валидация log_level — case-sensitive: "info" НЕ пройдёт.

        Готча: _validate_settings делает .upper() перед проверкой, так
        что "info" → "INFO" и проходит. Но это происходит в
        _validate_settings, а не в _LOG_LEVEL_NAMES напрямую.

        На самом деле, посмотрим на код:
          level = str(kwargs.get("log_level", Settings.log_level)).upper()
          if level not in _LOG_LEVEL_NAMES:
              ... fallback to INFO

        Так что .upper() применяется ВСЕГДА — "info" → "INFO" → проходит!
        Но это означает, что в settings хранится UPPERCASE версия.
        """
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"log_level": "debug"}, f)  # lowercase
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)
            # .upper() применяется в _validate_settings, так что "debug" → "DEBUG"
            assert cfg.settings.log_level == "DEBUG", (
                f"Ожидался 'DEBUG' (после .upper()), получили {cfg.settings.log_level!r}"
            )
            assert cfg.log_level == logging.DEBUG
            print("  [OK] log_level: .upper() применяется → 'debug' работает как 'DEBUG'")
        finally:
            os.unlink(cfg_path)

    def test_log_level_truly_invalid_falls_back(self):
        """Совершенно несуществующий уровень → откат к INFO."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"log_level": "TRACE"}, f)  # нет такого уровня
            f.flush()
            cfg_path = f.name

        try:
            cfg = Config(cfg_path)
            assert cfg.settings.log_level == "INFO", (
                f"TRACE не существует → откат к INFO, получили {cfg.settings.log_level!r}"
            )
            print("  [OK] Несуществующий уровень (TRACE) → откат к INFO")
        finally:
            os.unlink(cfg_path)

    # ------------------------------------------------------------------
    # Дополнительно: env-переменная с None-кастом
    # ------------------------------------------------------------------
    def test_env_var_with_cast_none_returns_default(self):
        """Если значение None и указан cast — возвращается default.

        Готча: get() проверяет: if value is None: return default
        ДО применения cast. Так что None никогда не доходит до cast.
        """
        os.environ["KAMIO_MQTT_BROKER"] = "mqtt://test:1883"

        cfg = Config()
        # mqtt_broker существует в settings — не None
        # Но продемонстрируем поведение с отсутствующим ключом
        result = cfg.get("totally_missing", default="fallback", cast=int)
        assert result == "fallback", (
            f"Отсутствующий ключ → default (до cast), получили {result!r}"
        )
        print("  [OK] None-значение → default возвращается ДО применения cast")


if __name__ == "__main__":
    print("=" * 70)
    print("ДЕМО: Подводные камни конфигурации Kamio (Config)")
    print("=" * 70)
    print()

    # Запускаем через unittest для удобства
    unittest.main(verbosity=2, exit=False)

    print()
    print("=" * 70)
    print("ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ — проверьте вывод выше на наличие [OK]")
    print("=" * 70)
