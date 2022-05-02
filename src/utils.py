from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO, SU
import calendar


def next_weekday(date, weekday):
    day_gap = weekday - date.weekday()
    if day_gap <= 0:
        day_gap += 7
    return date + timedelta(days=day_gap)


def split_date(from_date, to_date):
    from_datetime, to_datetime = datetime.strptime(from_date, "%Y-%m-%d"), datetime.strptime(to_date, "%Y-%m-%d")

    months, weeks, days = [], [], []
    # --- Get full months. ---
    monthly_from_date = from_datetime + relativedelta(months=1, day=1) if from_datetime.day != 1 else from_datetime

    # Check if to_date day is not the last day of the month.
    if to_datetime.day != calendar.monthrange(to_datetime.year, to_datetime.month)[1]:
        monthly_to_date = to_datetime - relativedelta(months=1, day=31)
    else:
        monthly_to_date = to_datetime

    # Get number of months.
    delta = relativedelta(monthly_to_date, monthly_from_date)
    months_num = (delta.years * 12) + delta.months

    monthly_residuals = []
    if months_num > 0:
        months.append((months_num, (monthly_from_date.strftime("%Y-%m-%d"), monthly_to_date.strftime("%Y-%m-%d"))))

        if from_datetime == monthly_from_date and to_datetime == monthly_to_date:
            return months, weeks, days

        if monthly_from_date != from_datetime:
            monthly_residuals.append((from_datetime, monthly_from_date - relativedelta(days=1)))
        if monthly_to_date != to_datetime:
            monthly_residuals.append((monthly_to_date + relativedelta(days=1), to_datetime))
    else:
        monthly_residuals.append((from_datetime, to_datetime))

    # --- Get full weeks. ---
    weekly_residuals = []
    for m_residuals_range in monthly_residuals:
        curr_from_datetime, curr_to_datetime = m_residuals_range

        # If range is less than 1 week.
        num_days = (curr_to_datetime - curr_from_datetime).days + 1
        if num_days < 7:
            weekly_residuals.append((curr_from_datetime, curr_to_datetime))
            continue

        # If range is more that 1 week.
        if curr_from_datetime.weekday() != 0:
            weekly_from_datetime = curr_from_datetime + relativedelta(weekday=MO(1))
        else:
            weekly_from_datetime = curr_from_datetime

        # If to datetime is not Sunday.
        if curr_to_datetime.weekday() != 6:
            weekly_to_datetime = curr_to_datetime + relativedelta(weekday=SU(-1))
        else:
            weekly_to_datetime = curr_to_datetime

        # Get num weeks.
        delta = (weekly_to_datetime + relativedelta(days=1) - weekly_from_datetime).days
        weeks_num = delta // 7

        weeks.append((weeks_num, (weekly_from_datetime.strftime("%Y-%m-%d"), weekly_to_datetime.strftime("%Y-%m-%d"))))

        if weekly_from_datetime != curr_from_datetime:
            weekly_residuals.append((curr_from_datetime, weekly_from_datetime - relativedelta(days=1)))
        if weekly_to_datetime != curr_to_datetime:
            weekly_residuals.append((weekly_to_datetime + relativedelta(days=1), curr_to_datetime))

    # --- Get full days. ---
    for w_residuals_range in weekly_residuals:
        curr_from_datetime, curr_to_datetime = w_residuals_range

        days_num = (curr_to_datetime + relativedelta(days=1) - curr_from_datetime).days
        days.append((days_num, (curr_from_datetime.strftime("%Y-%m-%d"), curr_to_datetime.strftime("%Y-%m-%d"))))
    return months, weeks, days
