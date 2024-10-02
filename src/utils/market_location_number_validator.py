import math


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
            raise ValueError(
                "Market Location numbers always have 11 digits."
            )

    def _validate_check_digit(self, value):
        if not str(value).isdigit():
            raise ValueError(
                "Market Location number must only contain digits."
            )
        digits = [int(d) for d in str(value)]
        check_digit = digits[-1]
        actual_number_digits = digits[:-1]
        computed_check_digit = self._compute_check_digit(actual_number_digits)
        if check_digit != computed_check_digit:
            raise ValueError(
                f"Wrong check sum. Expected {computed_check_digit}, got {check_digit}."
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