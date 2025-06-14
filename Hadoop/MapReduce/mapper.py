#!/usr/bin/env python3

import sys
from datetime import datetime

#################################################################################################

PAYMENT_MAP = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip',
}

#################################################################################################

def execute_mapping(payment_map) -> None:
    """Итерирование по полям csv файлов и печать в sys.stdout нужных значений."""

    for input_line in sys.stdin:
        row_values = input_line.strip().split(',')
        pickup_date = row_values[1].strip()
        payment_id = row_values[9].strip()
        tip_amount = row_values[13].strip()
        try:
            curr_date = datetime.strptime(pickup_date.split()[0], '%Y-%m-%d')
            if curr_date.year == 2020:
                payment_type = payment_map.get(payment_id, False)
                if payment_type:
                    tip_amount = float(tip_amount)
                    print(f'{curr_date.month}\t{payment_type}\t{tip_amount}')
        except:
            continue

#################################################################################################

if __name__ == '__main__':
    execute_mapping(PAYMENT_MAP)
