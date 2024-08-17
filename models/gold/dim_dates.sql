with
    dates_raw as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2011-01-01' as date)",
        end_date="cast('2014-12-31' as date)"
        )
    }}
    )

    , days_info as (
        select
            cast(date_day as date) as metric_date
            , extract(dayofweek from date_day) as week_day
            , extract(month from date_day) as month_number
            , extract(quarter from date_day) as quarter
            , extract(year from date_day) as date_year
            , format_date('%B', date_day) as month_name
            , format_date('%d-%m', date_day) as day_month
        from dates_raw
    )

    , days_named as (
        select
            *
            , case
                when week_day = 1 then 'Sunday'
                when week_day = 2 then 'Monday'
                when week_day = 3 then 'Tuesday'
                when week_day = 4 then 'Wednesday'
                when week_day = 5 then 'Thursday'
                when week_day = 6 then 'Friday'
                when week_day = 7 then 'Saturday'
            end as day_name
            , case
                when month_number = 1 then 'Jan'
                when month_number = 2 then 'Feb'
                when month_number = 3 then 'Mar'
                when month_number = 4 then 'Apr'
                when month_number = 5 then 'May'
                when month_number = 6 then 'Jun'
                when month_number = 7 then 'Jul'
                when month_number = 8 then 'Aug'
                when month_number = 9 then 'Sep'
                when month_number = 10 then 'Oct'
                when month_number = 11 then 'Nov'
                else 'Dec'
            end as month_abrev
            , case
                when quarter = 1 then 'Q1'
                when quarter = 2 then 'Q2'
                when quarter = 3 then 'Q3'
                else 'Q4'
            end as quarter_name
            , case
                when quarter in(1,2) then 1
                else 2
            end as semester
            , case
                when quarter in(1,2) then 'First half'
                else 'Second half'
            end as semester_name
        from days_info
    ) 

    , select_columns as (
        select
            metric_date
            , week_day
            , day_name
            , month_number
            , month_name
            , month_abrev
            , quarter
            , quarter_name
            , semester
            , semester_name
            , date_year
            , day_month
            , concat(month_abrev, '-', date_year ) as month_year
        from days_named
    )   


select *
from select_columns 