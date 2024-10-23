import random
import string

import math

from src.utils.exceptions import ValidationError


def validate_market_or_metering_location_number(v):
    try:
        MeteringLocationNumberValidator()(v)
    except ValidationError:
        pass
    MarketLocationNumberValidator()(v)
    return v


class MarketLocationNumberGenerator:
    def __call__(self, *args, **kwargs):
        return self._generate()

    def _generate(self):
        number = random.randint(1000000000, 9999999999)
        digits = [int(d) for d in str(number)]
        check_digit = MarketLocationNumberValidator._compute_check_digit(digits)
        return str(number) + str(check_digit)


class MeteringLocationNumberGenerator:
    def __call__(self, *args, **kwargs):
        return self._generate()

    def _generate(self):
        return "DE" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=31))


# copied from optinode


class MarketLocationNumberValidator:
    """
    For example, 41373559241

    See Page 7 of https://bdew-codes.de/Content/Files/MaLo/2017-04-28-BDEW-Anwendungshilfe-MaLo-ID_Version1.0_FINAL.PDF
    """

    def __call__(self, value):
        self._validate_length(value)
        self._validate_check_digit(value)

    def _validate_length(self, value):
        if len(str(value)) != 11:
            raise ValidationError(
                "Market Location numbers always have 11 digits."
            )

    def _validate_check_digit(self, value):
        if not str(value).isdigit():
            raise ValidationError(
                "Market Location number must only contain digits."
            )
        digits = [int(d) for d in str(value)]
        check_digit = digits[-1]
        actual_number_digits = digits[:-1]
        computed_check_digit = self._compute_check_digit(actual_number_digits)
        if check_digit != computed_check_digit:
            raise ValidationError(
                f"Wrong check sum. Expected last digit to be {computed_check_digit}, got {check_digit}."
            )

    @staticmethod
    def _compute_check_digit(digits):
        a = sum(digits[::2])
        b = 2 * sum(digits[1::2])
        c = a + b
        c_next_multiple_of_ten = int(math.ceil(c / 10) * 10)
        d = c_next_multiple_of_ten - c
        if d == 10:
            return 0
        return d


class MeteringLocationNumberValidator:
    """
    For example, DE00056266802AO6G56M11SN51G21M24S
    """

    def __call__(self, value):
        self._validate_length(value)
        self._validate_german(value)

    def _validate_length(self, value):
        if len(value) != 33:
            raise ValidationError("Metering Location Number must consist of 33 symbols")

    def _validate_german(self, value):
        if value[:2] != "DE":
            raise ValidationError("Metering Location Number must start with 'DE'")