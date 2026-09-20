import math


def calculate_dew_point(temperature: float, humidity: float) -> float:
    """
    Calculate dew point using the Magnus approximation.

    Temperature is in Celsius.
    Humidity is relative humidity (%).
    Returns dew point in Celsius.
    """

    a = 17.62
    b = 243.12

    gamma = (
        math.log(humidity / 100)
        + (a * temperature) / (b + temperature)
    )

    dew_point = (b * gamma) / (a - gamma)
    return round(dew_point, 2)


def calculate_heat_index(
    temperature: float,
    humidity: float,
) -> float:
    """
    Calculate heat index.

    Temperature is in Celsius.
    Humidity is relative humidity (%).
    Returns heat index in Celsius.
    """

    temperature_f = (temperature * 9 / 5) + 32

    heat_index_f = (
        -42.379
        + 2.04901523 * temperature_f
        + 10.14333127 * humidity
        - 0.22475541 * temperature_f * humidity
        - 0.00683783 * temperature_f ** 2
        - 0.05481717 * humidity ** 2
        + 0.00122874 * temperature_f ** 2 * humidity
        + 0.00085282 * temperature_f * humidity ** 2
        - 0.00000199 * temperature_f ** 2 * humidity ** 2
    )

    heat_index_c = (heat_index_f - 32) * 5 / 9

    return round(heat_index_c, 2)