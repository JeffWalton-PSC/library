import pandas as pd
import datetime as dt
import local_db


START_ACADEMIC_YEAR = '2004'

def select(table:str, fields:list=None, where:str="", distinct=False, **kwargs) -> pd.DataFrame:
    """
    Function pulls data from PowerCampus database.

    Returns a pandas DataFrame.

    Example Usage:
    pc.select("ACADEMIC", 
          fields=['PEOPLE_CODE_ID', 'ACADEMIC_YEAR', 'ACADEMIC_TERM'], 
          where="ACADEMIC_YEAR='2021' and ACADEMIC_TERM='FALL' and CREDITS>0", 
          distinct=True)

    """
    
    connection = local_db.connection()

    if fields is None:
        fields = "*"
    else:
        fields = ", ".join(fields)

    if where != "":
        where = "WHERE " + where

    if distinct:
        distinct = "DISTINCT "
    else:
        distinct = ""
    
    parsedates = None
    if kwargs:
        if 'parse_dates' in kwargs.keys():
            parsedates = kwargs['parse_dates']

    sql_str = (
        f"SELECT {distinct}{fields} "
        + f"FROM {table} "
        + where
    )
    # print(sql_str)
    return ( pd.read_sql_query(sql_str, connection, parse_dates=parsedates)
    )


def current_yearterm() -> pd.DataFrame:
    """
    Function returns current year/term information based on today's date.

    Returns dataframe containing:
        term - string,
        year - string,
        yearterm - string,
        start_of_term - datetime,
        end_of_term - datetime,
        yearterm_sort - string
    """

    df_cal = ( select("ACADEMICCALENDAR", 
                    fields=['ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
                            'START_DATE', 'END_DATE', 'FINAL_END_DATE'
                            ], 
                    where=f"ACADEMIC_YEAR>='{START_ACADEMIC_YEAR}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER')", 
                    distinct=True
                    )
              .groupby(['ACADEMIC_YEAR', 'ACADEMIC_TERM']).agg(
                  {'START_DATE': ['min'],
                   'END_DATE': ['max'],
                   'FINAL_END_DATE': ['max']
                  }
              ).reset_index()
             )
    df_cal.columns = df_cal.columns.droplevel(1)
    
    yearterm_sort = ( lambda r:
        r['ACADEMIC_YEAR'] + '01' if r['ACADEMIC_TERM']=='SPRING' else
        (r['ACADEMIC_YEAR'] + '02' if r['ACADEMIC_TERM']=='SUMMER' else
        (r['ACADEMIC_YEAR'] + '03' if r['ACADEMIC_TERM']=='FALL' else
        r['ACADEMIC_YEAR'] + '00'))
    )
    df_cal['yearterm_sort'] = df_cal.apply(yearterm_sort, axis=1)

    df_cal['yearterm'] = df_cal['ACADEMIC_YEAR'] + '.' +  df_cal['ACADEMIC_TERM'].str.title()

    df_cal = ( 
        df_cal.drop(
            columns=[
                'END_DATE'
                ]
            )
        .rename(
            columns={
                'ACADEMIC_YEAR': 'year', 
                'ACADEMIC_TERM': 'term', 
                'START_DATE': 'start_of_term', 
                'FINAL_END_DATE': 'end_of_term', 
                }
            )
        )

    return df_cal.loc[(df_cal['end_of_term'] >= dt.datetime.today())].sort_values(['end_of_term']).iloc[[0]]


def add_col_yearterm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'yearterm' column to dataframe df.
    """
    
    if ('ACADEMIC_YEAR' in df.columns) and ('ACADEMIC_TERM' in df.columns):
        df['yearterm'] = df['ACADEMIC_YEAR'] + '.' +  df['ACADEMIC_TERM'].str.title()
    else:
        # print("ERROR: columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
        raise KeyError("columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
    
    return df


def add_col_yearterm_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'yearterm_sort' column to dataframe df.
    """
    
    yearterm_sort = ( lambda r:
        r['ACADEMIC_YEAR'] + '01' if r['ACADEMIC_TERM']=='SPRING' else
        (r['ACADEMIC_YEAR'] + '02' if r['ACADEMIC_TERM']=='SUMMER' else
        (r['ACADEMIC_YEAR'] + '03' if r['ACADEMIC_TERM']=='FALL' else
        r['ACADEMIC_YEAR'] + '00'))
    )
    
    if ('ACADEMIC_YEAR' in df.columns) and ('ACADEMIC_TERM' in df.columns):
        df['yearterm_sort'] = df.apply(yearterm_sort, axis=1)

    else:
        # print("ERROR: columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
        raise KeyError("columns not found ['ACADEMIC_YEAR', 'ACADEMIC_TERM']")
    
    return df