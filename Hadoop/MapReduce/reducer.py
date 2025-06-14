#!/usr/bin/env python3

from copy import deepcopy
import sys

#################################################################################################

template = {
    f'{idx}': {
        'Credit card': 0,
        'Cash': 0,
        'No charge': 0,
        'Dispute': 0,
        'Unknown': 0,
        'Voided trip': 0
    } for idx in range(1, 13)
}
MONTHS_COUNTER = deepcopy(template)
MONTHS_TOTAL_SUM = deepcopy(template)

#################################################################################################

def execute_reduce() -> None:
    """Формирование агрегирующего файла по месяцам и средним суммам."""

    for input_line in sys.stdin:
        month, payment_type, tip_amount = input_line.strip().split('\t')
        MONTHS_COUNTER[month][payment_type] = MONTHS_COUNTER[month][payment_type] + 1
        MONTHS_TOTAL_SUM[month][payment_type] = MONTHS_TOTAL_SUM[month][payment_type] + float(tip_amount)

    print('Month\tPayment type\tTips average amount')

    for month_values, sum_values in zip(
        MONTHS_COUNTER.items(),
        MONTHS_TOTAL_SUM.items(),
    ):
        month = f'2020-{month_values[0]}' if int(month_values[0]) > 9 else f'2020-0{month_values[0]}'
        for payment_counter, total_sums in zip(
            month_values[-1].items(),
            sum_values[-1].items()
        ):
            try:
                payment_type = payment_counter[0]
                avg_tip_amount = total_sums[-1] / payment_counter[-1]
                print(f'{month}\t{payment_type}\t{round(avg_tip_amount, 2)}')
            except:
                continue

#################################################################################################

if __name__ == '__main__':
    execute_reduce()
